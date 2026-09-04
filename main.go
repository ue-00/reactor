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
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	botToken = os.Getenv("TRAQ_BOT_TOKEN")
	baseURL  = "https://q.trap.jp/api/v3"

	userCache     = map[string]string{} // traQ ID -> UUID
	stampCache    = map[string]string{} // Stamp Name -> UUID
	stampIdToName = map[string]string{} // UUID -> Stamp Name

	// キャッシュをHTTPリクエストとバッチで安全に共有するためのMutex
	cacheMu sync.RWMutex

	// 安全のためにタイムアウトを設定したHTTPクライアント
	httpClient = &http.Client{
		Timeout: 10 * time.Second,
	}
)

// --- DBモデル ---

// ユーザーが登録したスタンプ
type UserStamp struct {
	ID      uint   `gorm:"primaryKey"`
	TraqID  string `gorm:"uniqueIndex:idx_user_stamp;size:36"`
	StampID string `gorm:"uniqueIndex:idx_user_stamp;size:36"`
}

// 同じメッセージを同じユーザーへ二重通知しないための記録
type NotificationLog struct {
	ID        uint      `gorm:"primaryKey"`
	MessageID string    `gorm:"uniqueIndex:idx_message_user;size:36"`
	TraqID    string    `gorm:"uniqueIndex:idx_message_user;size:36"`
	CreatedAt time.Time
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

type MessageSearchResponse struct {
	TotalHits int64     `json:"totalHits"`
	Hits      []Message `json:"hits"`
}

func main() {
	// DB接続 (NeoShowcaseの環境変数を読み込む)
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
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

	// DBマイグレーション
	if err := db.AutoMigrate(&UserStamp{}, &NotificationLog{}); err != nil {
		log.Fatalf("failed to migrate database: %v", err)
	}

	log.Println("Database connection and migration successful.")

	// 初回キャッシュ取得
	updateCache()

	// 起動直後にも直近8分のメッセージをチェック
	log.Println("Running initial message check...")
	checkMessagesAndSendDM(db)

	// 定期実行バッチ
	// 7分ごとに実行し、直近8分を見る
	go func() {
		log.Println("Starting 7-minute polling batch...")

		ticker := time.NewTicker(7 * time.Minute)
		defer ticker.Stop()

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

		// =========================
		// POST: スタンプの登録
		// =========================
		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil {
				log.Printf("Failed to parse form: %v", err)

				http.Redirect(
					w,
					r,
					"/?status=error",
					http.StatusSeeOther,
				)
				return
			}

			// 前後の空白と「:」を取り除く
			stampName := strings.TrimSpace(
				strings.Trim(r.FormValue("stamp_name"), ":"),
			)

			log.Printf(
				"Received registration request from user:%s for stamp:%s",
				traqID,
				stampName,
			)

			// スタンプ名からUUIDを取得
			stampID, ok := getStampID(stampName)

			if !ok {
				log.Printf(
					"Stamp:%s not found in cache",
					stampName,
				)

				// エラーメッセージを表示
				http.Redirect(
					w,
					r,
					"/?status=not_found",
					http.StatusSeeOther,
				)
				return
			}

			// すでに登録されているか確認
			var count int64

			if err := db.Model(&UserStamp{}).
				Where(
					"traq_id = ? AND stamp_id = ?",
					traqID,
					stampID,
				).
				Count(&count).Error; err != nil {

				log.Printf(
					"Failed to check existing stamp registration: %v",
					err,
				)

				http.Redirect(
					w,
					r,
					"/?status=error",
					http.StatusSeeOther,
				)
				return
			}

			// すでに登録済み
			if count > 0 {
				log.Printf(
					"Stamp:%s is already registered for user:%s",
					stampName,
					traqID,
				)

				http.Redirect(
					w,
					r,
					"/?status=already",
					http.StatusSeeOther,
				)
				return
			}

			// 登録
			record := &UserStamp{
				TraqID:  traqID,
				StampID: stampID,
			}

			if err := db.Create(record).Error; err != nil {
				log.Printf(
					"Failed to register stamp:%s for user:%s: %v",
					stampName,
					traqID,
					err,
				)

				http.Redirect(
					w,
					r,
					"/?status=error",
					http.StatusSeeOther,
				)
				return
			}

			log.Printf(
				"Successfully registered stamp:%s (ID:%s) for user:%s",
				stampName,
				stampID,
				traqID,
			)

			http.Redirect(
				w,
				r,
				"/?status=success",
				http.StatusSeeOther,
			)
			return
		}

		// =========================
		// GET: ステータスメッセージ
		// =========================

		status := r.URL.Query().Get("status")

		statusMessage := ""
		statusClass := ""

		switch status {
		case "success":
			statusMessage = "スタンプを登録しました。"
			statusClass = "success"

		case "not_found":
			statusMessage = "スタンプ名が違います。正しいスタンプ名を入力してください。"
			statusClass = "error"

		case "already":
			statusMessage = "このスタンプはすでに登録されています。"
			statusClass = "warning"

		case "error":
			statusMessage = "登録中にエラーが発生しました。"
			statusClass = "error"
		}

		// =========================
		// GET: 登録済みスタンプ一覧
		// =========================

		var userStamps []UserStamp

		if err := db.
			Where("traq_id = ?", traqID).
			Find(&userStamps).Error; err != nil {

			log.Printf(
				"Failed to fetch registered stamps for user:%s: %v",
				traqID,
				err,
			)

			http.Error(
				w,
				"Database error",
				http.StatusInternalServerError,
			)
			return
		}

		type StampInfo struct {
			ID   uint
			Name string
		}

		var stamps []StampInfo

		for _, us := range userStamps {
			stampName, ok := getStampName(us.StampID)

			if !ok {
				stampName = "(不明なスタンプ)"
			}

			stamps = append(stamps, StampInfo{
				ID:   us.ID,
				Name: stampName,
			})
		}

		// =========================
		// HTML
		// =========================

		tmpl := `<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">

<title>ReaQtion</title>

<link rel="icon" href="icon.svg">

<style>
  body {
    background: #f5f5f5;
    max-width: 800px;
    margin: 0 auto;
    padding: 16px;
    box-sizing: border-box;
    font-family: sans-serif;
    color: #333;
  }

  .title {
    display: flex;
    align-items: center;
    font-size: 24px;
    font-weight: bold;
    margin: 0 0 24px;
  }

  .title-icon {
    width: 256px;
    margin-right: 8px;
  }

  .description {
    font-size: 14px;
    color: #666;
    margin-bottom: 24px;
    line-height: 1.7;
  }

  h2 {
    font-size: 18px;
    margin: 24px 0 12px;
  }

  .status-message {
    padding: 10px 12px;
    margin-bottom: 16px;
    border-radius: 6px;
    font-size: 13px;
    line-height: 1.5;
  }

  .status-success {
    background: #e8f5e9;
    border: 1px solid #a5d6a7;
    color: #2e7d32;
  }

  .status-error {
    background: #ffebee;
    border: 1px solid #ef9a9a;
    color: #c62828;
  }

  .status-warning {
    background: #fff8e1;
    border: 1px solid #ffe082;
    color: #8d6e00;
  }

  .form-group {
    display: flex;
    gap: 8px;
    width: 100%;
  }

  #stamp_name {
    flex: 1;
    min-width: 0;
    font-size: 14px;
    padding: 10px;
    box-sizing: border-box;
    border: 1px solid #ccc;
    border-radius: 6px;
    background: #fff;
    outline: none;
  }

  #stamp_name:focus {
    border-color: #007bff;
  }

  #add {
    padding: 10px 18px;
    font-size: 14px;
    background: #007bff;
    color: #fff;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    white-space: nowrap;
  }

  #add:hover {
    background: #0056b3;
  }

  ul {
    list-style: none;
    padding-left: 0;
    margin: 0;
  }

  .stamp-item {
    display: flex;
    align-items: center;
    padding: 10px 12px;
    border: 1px solid #ddd;
    border-radius: 6px;
    margin-bottom: 6px;
    background: #f9f9f9;
    box-sizing: border-box;
  }

  .stamp-name {
    flex-grow: 1;
    min-width: 0;
    font-size: 14px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .delete-button {
    margin-left: 12px;
    padding: 6px 10px;
    font-size: 12px;
    border: 1px solid #ccc;
    border-radius: 6px;
    background: #fff;
    color: #555;
    cursor: pointer;
  }

  .delete-button:hover {
    background: #f0f0f0;
    color: #d00;
  }

  .empty-message {
    padding: 16px;
    border: 1px dashed #ccc;
    border-radius: 6px;
    background: #fafafa;
    color: #777;
    text-align: center;
    font-size: 13px;
  }

  @media (max-width: 500px) {
    .form-group {
      flex-direction: column;
    }

    #add {
      width: 100%;
    }

    .title {
      font-size: 21px;
    }
  }
</style>
</head>

<body>

  <h1 class="title">
    <img
      src="icon.svg"
      alt="icon"
      class="title-icon"
    >
  </h1>

  <p class="description">
    ようこそ、{{.TraqID}}さん。<br>
    登録したスタンプがメッセージに押されたとき、DMで通知します。
  </p>

  {{if .StatusMessage}}
    <div class="status-message status-{{.StatusClass}}">
      {{.StatusMessage}}
    </div>
  {{end}}

  <h2>スタンプを登録</h2>

  <form method="post" action="/">
    <div class="form-group">

      <input
        id="stamp_name"
        name="stamp_name"
        placeholder="スタンプ名（例: oisu-）"
        required
      >

      <button
        id="add"
        type="submit"
      >
        追加
      </button>

    </div>
  </form>

  <h2>登録済みスタンプ</h2>

  {{if .Stamps}}

    <ul>
      {{range .Stamps}}

        <li class="stamp-item">

          <span class="stamp-name">
            :{{.Name}}:
          </span>

          <form
            method="post"
            action="/delete"
          >
            <input
              type="hidden"
              name="id"
              value="{{.ID}}"
            >

            <button
              type="submit"
              class="delete-button"
            >
              削除
            </button>
          </form>

        </li>

      {{end}}
    </ul>

  {{else}}

    <div class="empty-message">
      登録されているスタンプはありません。
    </div>

  {{end}}

</body>
</html>`

		t, err := template.New("web").Parse(tmpl)
		if err != nil {
			log.Printf(
				"Failed to parse template: %v",
				err,
			)

			http.Error(
				w,
				"Template error",
				http.StatusInternalServerError,
			)
			return
		}

		if err := t.Execute(
			w,
			map[string]interface{}{
				"TraqID":        traqID,
				"Stamps":        stamps,
				"StatusMessage": statusMessage,
				"StatusClass":   statusClass,
			},
		); err != nil {
			log.Printf(
				"Failed to execute template: %v",
				err,
			)
		}
	})

	// =========================
	// スタンプの登録削除
	// =========================

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		traqID := r.Header.Get("X-Showcase-User")

		if r.Method == http.MethodPost {
			id := r.FormValue("id")

			log.Printf(
				"Received delete request from user:%s for record ID:%s",
				traqID,
				id,
			)

			if err := db.
				Where(
					"id = ? AND traq_id = ?",
					id,
					traqID,
				).
				Delete(&UserStamp{}).Error; err != nil {

				log.Printf(
					"Failed to delete stamp registration: %v",
					err,
				)
			}
		}

		http.Redirect(
			w,
			r,
			"/",
			http.StatusSeeOther,
		)
	})

	// =========================
	// HTTPサーバー起動
	// =========================

	port := os.Getenv("PORT")

	if port == "" {
		port = "8080"
	}

	log.Printf(
		"Server started on :%s",
		port,
	)

	log.Fatal(
		http.ListenAndServe(
			":"+port,
			nil,
		),
	)
}

// ============================================================
// キャッシュ操作
// ============================================================

func getStampID(stampName string) (string, bool) {
	cacheMu.RLock()
	defer cacheMu.RUnlock()

	id, ok := stampCache[stampName]
	return id, ok
}

func getStampName(stampID string) (string, bool) {
	cacheMu.RLock()
	defer cacheMu.RUnlock()

	name, ok := stampIdToName[stampID]
	return name, ok
}

func getUserUUID(traqID string) (string, bool) {
	cacheMu.RLock()
	defer cacheMu.RUnlock()

	uuid, ok := userCache[traqID]
	return uuid, ok
}

// ============================================================
// キャッシュ更新
// ============================================================

func updateCache() {
	log.Println("Updating user and stamp caches...")

	// -------------------------
	// ユーザー一覧
	// -------------------------

	newUserCache := make(map[string]string)

	reqU, err := http.NewRequest(
		http.MethodGet,
		baseURL+"/users",
		nil,
	)

	if err != nil {
		log.Printf(
			"Failed to create users request: %v",
			err,
		)
	} else {
		reqU.Header.Set(
			"Authorization",
			"Bearer "+botToken,
		)

		resp, err := httpClient.Do(reqU)

		if err != nil {
			log.Printf(
				"Failed to fetch users: %v",
				err,
			)
		} else {
			if resp.StatusCode < 200 ||
				resp.StatusCode >= 300 {

				log.Printf(
					"Failed to fetch users: HTTP %d",
					resp.StatusCode,
				)

				resp.Body.Close()
			} else {
				var users []User

				err := json.NewDecoder(
					resp.Body,
				).Decode(&users)

				resp.Body.Close()

				if err != nil {
					log.Printf(
						"Failed to decode users response: %v",
						err,
					)
				} else {
					for _, u := range users {
						newUserCache[u.Name] = u.ID
					}

					cacheMu.Lock()
					userCache = newUserCache
					cacheMu.Unlock()

					log.Printf(
						"User cache updated: %d users",
						len(newUserCache),
					)
				}
			}
		}
	}

	// -------------------------
	// スタンプ一覧
	// -------------------------

	newStampCache := make(map[string]string)
	newStampIdToName := make(map[string]string)

	reqS, err := http.NewRequest(
		http.MethodGet,
		baseURL+"/stamps",
		nil,
	)

	if err != nil {
		log.Printf(
			"Failed to create stamps request: %v",
			err,
		)
	} else {
		reqS.Header.Set(
			"Authorization",
			"Bearer "+botToken,
		)

		resp, err := httpClient.Do(reqS)

		if err != nil {
			log.Printf(
				"Failed to fetch stamps: %v",
				err,
			)
		} else {
			if resp.StatusCode < 200 ||
				resp.StatusCode >= 300 {

				log.Printf(
					"Failed to fetch stamps: HTTP %d",
					resp.StatusCode,
				)

				resp.Body.Close()
			} else {
				var stamps []Stamp

				err := json.NewDecoder(
					resp.Body,
				).Decode(&stamps)

				resp.Body.Close()

				if err != nil {
					log.Printf(
						"Failed to decode stamps response: %v",
						err,
					)
				} else {
					for _, s := range stamps {
						newStampCache[s.Name] = s.ID
						newStampIdToName[s.ID] = s.Name
					}

					cacheMu.Lock()
					stampCache = newStampCache
					stampIdToName = newStampIdToName
					cacheMu.Unlock()

					log.Printf(
						"Stamp cache updated: %d stamps",
						len(newStampCache),
					)
				}
			}
		}
	}
}

// ============================================================
// メッセージ検索・通知
// ============================================================

func checkMessagesAndSendDM(db *gorm.DB) {
	// 7分ごとに実行するが、
	// 直近8分まで検索する。
	//
	// 1分ぶん検索範囲を重複させることで、
	// 実行タイミングのズレによる取りこぼしを防ぐ。
	since := time.Now().
		Add(-8 * time.Minute).
		UTC().
		Format(time.RFC3339)

	v := url.Values{}

	v.Add(
		"limit",
		"100",
	)

	v.Add(
		"after",
		since,
	)

	searchURL := fmt.Sprintf(
		"%s/messages?%s",
		baseURL,
		v.Encode(),
	)

	log.Printf(
		"Fetching messages URL: %s",
		searchURL,
	)

	req, err := http.NewRequest(
		http.MethodGet,
		searchURL,
		nil,
	)

	if err != nil {
		log.Printf(
			"Failed to create messages request: %v",
			err,
		)
		return
	}

	req.Header.Set(
		"Authorization",
		"Bearer "+botToken,
	)

	resp, err := httpClient.Do(req)

	if err != nil {
		log.Printf(
			"Search API error: %v",
			err,
		)
		return
	}

	defer resp.Body.Close()

	// HTTPステータスチェック
	if resp.StatusCode < 200 ||
		resp.StatusCode >= 300 {

		log.Printf(
			"Search API returned status: %d",
			resp.StatusCode,
		)
		return
	}

	// APIレスポンス:
	//
	// {
	//   "totalHits": 10000,
	//   "hits": [...]
	// }
	var result MessageSearchResponse

	if err := json.NewDecoder(
		resp.Body,
	).Decode(&result); err != nil {

		log.Printf(
			"Failed to decode messages response: %v",
			err,
		)
		return
	}

	messages := result.Hits

	log.Printf(
		"Found %d messages in the last 4 minutes (totalHits=%d)",
		len(messages),
		result.TotalHits,
	)

	for _, msg := range messages {
		processMessage(
			db,
			msg,
		)
	}
}

// ============================================================
// 1メッセージの処理
// ============================================================

func processMessage(
	db *gorm.DB,
	msg Message,
) {
	// 1メッセージ内で同じスタンプが
	// 複数回登場しても1回だけ扱う
	seenStampsInMsg := map[string]bool{}

	// ユーザーごとに一致したスタンプ名をまとめる
	// traqID -> []stampName
	userMatchedStamps := map[string][]string{}

	for _, s := range msg.Stamps {
		if seenStampsInMsg[s.StampID] {
			continue
		}

		seenStampsInMsg[s.StampID] = true

		var targets []UserStamp

		if err := db.
			Where(
				"stamp_id = ?",
				s.StampID,
			).
			Find(&targets).Error; err != nil {

			log.Printf(
				"Failed to find users for stamp:%s on message:%s: %v",
				s.StampID,
				msg.ID,
				err,
			)
			continue
		}

		name, ok := getStampName(
			s.StampID,
		)

		if !ok {
			log.Printf(
				"Stamp name not found for stamp ID:%s",
				s.StampID,
			)
			continue
		}

		for _, target := range targets {
			userMatchedStamps[target.TraqID] =
				append(
					userMatchedStamps[target.TraqID],
					":"+name+":",
				)
		}
	}

	// ユーザーごとに1通にまとめてDM送信
	for traqID, stampNames := range userMatchedStamps {
		sendNotificationToUser(
			db,
			msg,
			traqID,
			stampNames,
		)
	}
}

// ============================================================
// ユーザーへの通知
// ============================================================

func sendNotificationToUser(
	db *gorm.DB,
	msg Message,
	traqID string,
	stampNames []string,
) {
	// すでにこのユーザーへこのメッセージを
	// 通知済みか確認
	var count int64

	if err := db.
		Model(&NotificationLog{}).
		Where(
			"message_id = ? AND traq_id = ?",
			msg.ID,
			traqID,
		).
		Count(&count).Error; err != nil {

		log.Printf(
			"Failed to check notification log for user:%s message:%s: %v",
			traqID,
			msg.ID,
			err,
		)
		return
	}

	if count > 0 {
		log.Printf(
			"Notification already sent to user:%s for message:%s",
			traqID,
			msg.ID,
		)
		return
	}

	// traQ ID -> UUID
	userUUID, ok := getUserUUID(traqID)

	if !ok {
		log.Printf(
			"User UUID not found for traq_id:%s",
			traqID,
		)
		return
	}

	stampsText := strings.Join(
		stampNames,
		" ",
	)

	dmContent := fmt.Sprintf(
		"あなたが登録したスタンプ ( %s ) が付いたメッセージがあります！\nhttps://q.trap.jp/messages/%s",
		stampsText,
		msg.ID,
	)

	log.Printf(
		"Sending DM to user:%s (UUID:%s) with stamps [%s] for message:%s",
		traqID,
		userUUID,
		stampsText,
		msg.ID,
	)

	// DM送信に成功した場合だけ通知済みとして記録
	if !sendDM(
		userUUID,
		dmContent,
	) {
		return
	}

	notificationLog := &NotificationLog{
		MessageID: msg.ID,
		TraqID:    traqID,
	}

	if err := db.Create(
		notificationLog,
	).Error; err != nil {

		log.Printf(
			"Failed to save notification log for user:%s message:%s: %v",
			traqID,
			msg.ID,
			err,
		)
		return
	}

	log.Printf(
		"Notification log saved for user:%s message:%s",
		traqID,
		msg.ID,
	)
}

// ============================================================
// DM送信
// ============================================================

func sendDM(
	userUUID string,
	content string,
) bool {
	apiURL := fmt.Sprintf(
		"%s/users/%s/messages",
		baseURL,
		userUUID,
	)

	body, err := json.Marshal(
		map[string]interface{}{
			"content": content,
			"embed":   true,
		},
	)

	if err != nil {
		log.Printf(
			"Failed to encode DM body for %s: %v",
			userUUID,
			err,
		)
		return false
	}

	req, err := http.NewRequest(
		http.MethodPost,
		apiURL,
		bytes.NewBuffer(body),
	)

	if err != nil {
		log.Printf(
			"Failed to create DM request for %s: %v",
			userUUID,
			err,
		)
		return false
	}

	req.Header.Set(
		"Authorization",
		"Bearer "+botToken,
	)

	req.Header.Set(
		"Content-Type",
		"application/json",
	)

	resp, err := httpClient.Do(req)

	if err != nil {
		log.Printf(
			"Error sending DM to %s: %v",
			userUUID,
			err,
		)

		return false
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 200 &&
		resp.StatusCode < 300 {

		log.Printf(
			"Successfully sent DM to %s",
			userUUID,
		)

		// APIへの連続アクセスを少し間隔を空ける
		time.Sleep(100 * time.Millisecond)

		return true
	}

	log.Printf(
		"Failed to send DM to %s, Status Code: %d",
		userUUID,
		resp.StatusCode,
	)

	time.Sleep(100 * time.Millisecond)

	return false
}