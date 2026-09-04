package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	botToken      = os.Getenv("TRAQ_BOT_TOKEN")
	baseURL       = "https://q.trap.jp/api/v3"
	userCache     = map[string]string{} // traQ ID -> UUID
	stampCache    = map[string]string{} // Stamp Name -> UUID
	stampIdToName = map[string]string{} // UUID -> Stamp Name
)

// --- DBモデル ---
type UserStamp struct {
	ID      uint   `gorm:"primaryKey"`
	TraqID  string `gorm:"index"`
	StampID string
}

// --- APIレスポンス用構造体 ---
type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type Stamp struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type SearchResponse struct {
	Hits []struct {
		ID     string `json:"id"`
		Stamps []struct {
			StampID string `json:"stampId"`
		} `json:"stamps"`
	} `json:"hits"`
}

func main() {
	// DB接続 (NeoShowcaseの環境変数を読み込む)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		os.Getenv("NS_MARIADB_USER"),
		os.Getenv("NS_MARIADB_PASSWORD"),
		os.Getenv("NS_MARIADB_HOSTNAME"),
		os.Getenv("NS_MARIADB_PORT"),
		os.Getenv("NS_MARIADB_DATABASE"),
	)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}
	db.AutoMigrate(&UserStamp{})

	// 初回キャッシュ取得
	updateCache()

	// 定期実行バッチ (1時間ごと)
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		for range ticker.C {
			updateCache()
			checkMessagesAndSendDM(db)
		}
	}()

	// --- Webページ & API ---
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// NeoShowcaseのHard認証によって付与されるヘッダー
		traqID := r.Header.Get("X-Showcase-User")

		// POST: スタンプの登録
		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err == nil {
				stampName := strings.Trim(r.FormValue("stamp_name"), ":")
				if stampID, ok := stampCache[stampName]; ok {
					// 既に登録されていないか確認
					var count int64
					db.Model(&UserStamp{}).Where("traq_id = ? AND stamp_id = ?", traqID, stampID).Count(&count)
					if count == 0 {
						db.Create(&UserStamp{TraqID: traqID, StampID: stampID})
					}
				}
			}
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}

		// GET: 登録済みスタンプ一覧の表示
		var userStamps []UserStamp
		db.Where("traq_id = ?", traqID).Find(&userStamps)

		type StampInfo struct {
			ID   uint
			Name string
		}
		var stamps []StampInfo
		for _, us := range userStamps {
			stamps = append(stamps, StampInfo{
				ID:   us.ID,
				Name: stampIdToName[us.StampID],
			})
		}

		tmpl := `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>スタンプDM通知Bot</title>
<style>
  body { font-family: sans-serif; max-width: 600px; margin: 2rem auto; padding: 0 1rem; }
  ul { padding-left: 0; list-style: none; }
  li { padding: 0.5rem; border-bottom: 1px solid #ccc; display: flex; justify-content: space-between; }
</style>
</head>
<body>
  <h1>ようこそ、{{.TraqID}}さん</h1>
  <p>ここでは、あなたが登録したスタンプを管理できます。</p>
  <h2>新しく登録</h2>
  <form method="post" action="/">
    <input type="text" name="stamp_name" placeholder="スタンプ名 (例: oisu-)" required>
    <button type="submit">登録</button>
  </form>
  <h2>登録済みスタンプ</h2>
  <ul>
  {{range .Stamps}}
    <li>:{{.Name}}:
      <form method="post" action="/delete" style="display:inline;">
        <input type="hidden" name="id" value="{{.ID}}">
        <button type="submit">削除</button>
      </form>
    </li>
  {{else}}
    <p>登録されているスタンプはありません。</p>
  {{end}}
  </ul>
</body>
</html>`
		t, _ := template.New("web").Parse(tmpl)
		t.Execute(w, map[string]interface{}{
			"TraqID": traqID,
			"Stamps": stamps,
		})
	})

	// スタンプの登録削除
	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		traqID := r.Header.Get("X-Showcase-User")
		if r.Method == http.MethodPost {
			id := r.FormValue("id")
			db.Where("id = ? AND traq_id = ?", id, traqID).Delete(&UserStamp{})
		}
		http.Redirect(w, r, "/", http.StatusSeeOther)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Server started on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// --- API操作・バッチロジック ---

func updateCache() {
	// ユーザー一覧の取得
	reqU, _ := http.NewRequest("GET", baseURL+"/users", nil)
	reqU.Header.Set("Authorization", "Bearer "+botToken)
	if resp, err := http.DefaultClient.Do(reqU); err == nil {
		defer resp.Body.Close()
		var users []User
		if json.NewDecoder(resp.Body).Decode(&users) == nil {
			for _, u := range users {
				userCache[u.Name] = u.ID
			}
		}
	}

	// スタンプ一覧の取得
	reqS, _ := http.NewRequest("GET", baseURL+"/stamps", nil)
	reqS.Header.Set("Authorization", "Bearer "+botToken)
	if resp, err := http.DefaultClient.Do(reqS); err == nil {
		defer resp.Body.Close()
		var stamps []Stamp
		if json.NewDecoder(resp.Body).Decode(&stamps) == nil {
			for _, s := range stamps {
				stampCache[s.Name] = s.ID
				stampIdToName[s.ID] = s.Name
			}
		}
	}
}

func checkMessagesAndSendDM(db *gorm.DB) {
	// 1時間前の時刻をRFC3339で指定
	since := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	url := fmt.Sprintf("%s/search/messages?limit=100&q=created:>=%s", baseURL, since)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+botToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("Search API error:", err)
		return
	}
	defer resp.Body.Close()

	var searchRes SearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&searchRes); err != nil {
		return
	}

	// 取得したメッセージ群からスタンプを検証
	for _, hit := range searchRes.Hits {
		for _, s := range hit.Stamps {
			var targets []UserStamp
			db.Where("stamp_id = ?", s.StampID).Find(&targets)
			for _, target := range targets {
				if userUUID, ok := userCache[target.TraqID]; ok {
					msg := fmt.Sprintf("あなたが登録したスタンプ (:%s:) が付いたメッセージがあります！\nhttps://q.trap.jp/messages/%s", stampIdToName[s.StampID], hit.ID)
					sendDM(userUUID, msg)
				}
			}
		}
	}
}

func sendDM(userUUID, content string) {
	url := fmt.Sprintf("%s/users/%s/messages", baseURL, userUUID)
	body, _ := json.Marshal(map[string]interface{}{
		"content": content,
		"embed":   true,
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+botToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
	// traQサーバーへの負荷対策として少し待つ
	time.Sleep(100 * time.Millisecond)
}
