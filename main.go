package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
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

	// 安全のためにタイムアウトを設定したHTTPクライアント
	httpClient = &http.Client{
		Timeout: 10 * time.Second,
	}
)

// --- DBモデル ---
type UserStamp struct {
	ID      uint   `gorm:"primaryKey"`
	TraqID  string `gorm:"uniqueIndex:idx_user_stamp;size:36"`
	StampID string `gorm:"uniqueIndex:idx_user_stamp;size:36"`
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
type Message struct {
	ID     string `json:"id"`
	Stamps []struct {
		StampID string `json:"stampId"`
	} `json:"stamps"`
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
	log.Println("Database connection and migration successful.")

	// 初回キャッシュ取得
	updateCache()

	// 定期実行バッチ (15分ごと)
	go func() {
		log.Println("Starting 15-minute polling batch...")
		ticker := time.NewTicker(15 * time.Minute)
		for range ticker.C {
			log.Println("--- Triggered polling batch ---")
			updateCache()
			checkMessagesAndSendDM(db)
			log.Println("--- Finished polling batch ---")
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
				log.Printf("Received registration request from user:%s for stamp:%s", traqID, stampName)
				if stampID, ok := stampCache[stampName]; ok {
					var count int64
					db.Model(&UserStamp{}).Where("traq_id = ? AND stamp_id = ?", traqID, stampID).Count(&count)
					if count == 0 {
						db.Create(&UserStamp{TraqID: traqID, StampID: stampID})
						log.Printf("Successfully registered stamp:%s (ID:%s) for user:%s", stampName, stampID, traqID)
					} else {
						log.Printf("Stamp:%s is already registered for user:%s", stampName, traqID)
					}
				} else {
					log.Printf("Stamp:%s not found in cache", stampName)
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
			log.Printf("Received delete request from user:%s for record ID:%s", traqID, id)
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
	log.Println("Updating user and stamp caches...")
	// ユーザー一覧の取得
	reqU, _ := http.NewRequest("GET", baseURL+"/users", nil)
	reqU.Header.Set("Authorization", "Bearer "+botToken)
	if resp, err := httpClient.Do(reqU); err == nil {
		defer resp.Body.Close()
		var users []User
		if json.NewDecoder(resp.Body).Decode(&users) == nil {
			for _, u := range users {
				userCache[u.Name] = u.ID
			}
			log.Printf("User cache updated: %d users", len(userCache))
		} else {
			log.Println("Failed to decode users response")
		}
	} else {
		log.Printf("Failed to fetch users: %v", err)
	}

	// スタンプ一覧の取得
	reqS, _ := http.NewRequest("GET", baseURL+"/stamps", nil)
	reqS.Header.Set("Authorization", "Bearer "+botToken)
	if resp, err := httpClient.Do(reqS); err == nil {
		defer resp.Body.Close()
		var stamps []Stamp
		if json.NewDecoder(resp.Body).Decode(&stamps) == nil {
			for _, s := range stamps {
				stampCache[s.Name] = s.ID
				stampIdToName[s.ID] = s.Name
			}
			log.Printf("Stamp cache updated: %d stamps", len(stampCache))
		} else {
			log.Println("Failed to decode stamps response")
		}
	} else {
		log.Printf("Failed to fetch stamps: %v", err)
	}
}

func checkMessagesAndSendDM(db *gorm.DB) {
	// 15分前の時刻をRFC3339（UTC）で指定
	since := time.Now().Add(-15 * time.Minute).UTC().Format(time.RFC3339)

	v := url.Values{}
	v.Add("limit", "100")
	v.Add("after", since)

	searchURL := fmt.Sprintf("%s/messages?%s", baseURL, v.Encode())
	log.Printf("Fetching messages URL: %s", searchURL)

	req, _ := http.NewRequest("GET", searchURL, nil)
	req.Header.Set("Authorization", "Bearer "+botToken)
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("Search API error:", err)
		return
	}
	defer resp.Body.Close()

	var messages []Message
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		log.Println("Failed to decode messages response:", err)
		return
	}

	log.Printf("Found %d messages in the last 15 minutes", len(messages))

	for _, msg := range messages {
		// 1メッセージ内で重複するスタンプ判定を避けるセット
		seenStampsInMsg := map[string]bool{}

		// ユーザーごとに一致したスタンプ名をまとめるマップ: traqID -> []stampName
		userMatchedStamps := map[string][]string{}

		for _, s := range msg.Stamps {
			if seenStampsInMsg[s.StampID] {
				continue
			}
			seenStampsInMsg[s.StampID] = true

			var targets []UserStamp
			db.Where("stamp_id = ?", s.StampID).Find(&targets)
			for _, target := range targets {
				name := stampIdToName[s.StampID]
				userMatchedStamps[target.TraqID] = append(userMatchedStamps[target.TraqID], ":"+name+":")
			}
		}

		// ユーザーごとに1通にまとめてDM送信
		for traqID, stampNames := range userMatchedStamps {
			userUUID, ok := userCache[traqID]
			if !ok {
				log.Printf("User UUID not found for traq_id:%s", traqID)
				continue
			}

			stampsText := strings.Join(stampNames, " ")
			dmContent := fmt.Sprintf("登録したスタンプ ( %s ) が付いたメッセージがあります！\nhttps://q.trap.jp/messages/%s", stampsText, msg.ID)

			log.Printf("Sending DM to user:%s (UUID:%s) with stamps [%s] for message:%s", traqID, userUUID, stampsText, msg.ID)
			sendDM(userUUID, dmContent)
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

	resp, err := httpClient.Do(req)
	if err == nil {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Printf("Successfully sent DM to %s", userUUID)
		} else {
			log.Printf("Failed to send DM to %s, Status Code: %d", userUUID, resp.StatusCode)
		}
		resp.Body.Close()
	} else {
		log.Printf("Error sending DM to %s: %v", userUUID, err)
	}
	time.Sleep(100 * time.Millisecond)
}