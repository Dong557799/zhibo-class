package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	DBUser     string `json:"db_user"`
	DBPassword string `json:"db_password"`
	DBHost     string `json:"db_host"`
	DBPort     int    `json:"db_port"`
	DBName     string `json:"db_name"`
	LivegoURL  string `json:"livego_url"`
	APIPort    int    `json:"api_port"`
}

// 直播会话
type LiveSession struct {
	ID        int               `json:"id"`
	CourseID  int               `json:"course_id"`
	StreamKey string            `json:"stream_key"`
	Status    string            `json:"status"`
	StartTime time.Time         `json:"start_time,omitempty"`
	EndTime   time.Time         `json:"end_time,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	PlayURLs  map[string]string `json:"play_urls,omitempty"`
}

// 题目结构体
type Question struct {
	ID       int      `json:"id"`
	CourseID int      `json:"course_id"`
	Type     string   `json:"type"` // 题目类型，如选择题、判断题
	Content  string   `json:"content"`
	Options  []string `json:"options,omitempty"` // 选择题选项
	Answer   string   `json:"answer"`
}

var (
	db     *sql.DB
	config Config
)

func main() {
	// 加载配置
	if err := loadConfig(); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 连接数据库
	var err error
	db, err = connectDB()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// 测试数据库连接
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// 初始化路由
	r := initRouter()

	// 启动服务
	log.Printf("Starting live service on port %d", config.APIPort)
	if err := r.Run(fmt.Sprintf(":%d", config.APIPort)); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func loadConfig() error {
	file, err := os.Open("config.json")
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	return decoder.Decode(&config)
}

func connectDB() (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.DBUser,
		config.DBPassword,
		config.DBHost,
		config.DBPort,
		config.DBName)

	return sql.Open("mysql", dsn)
}

func initRouter() *gin.Engine {
	r := gin.Default()

	// 直播会话管理
	liveGroup := r.Group("/api/live")
	{
		liveGroup.POST("/sessions", createLiveSession)
		liveGroup.GET("/sessions/:id", getLiveSession)
		liveGroup.POST("/sessions/:id/start", startLiveSession)
		liveGroup.POST("/sessions/:id/end", endLiveSession)
	}

	// 直播状态回调
	r.POST("/api/live/status", handleLiveStatusCallback)

	// 在线答题管理
	questionGroup := r.Group("/api/question")
	{
		questionGroup.POST("/create", createQuestion)
		questionGroup.GET("/push/:course_id/:question_id", pushQuestion)
		questionGroup.POST("/submit", submitAnswer)
		questionGroup.GET("/result/:question_id", getResult)
	}

	return r
}

// 创建直播会话
func createLiveSession(c *gin.Context) {
	var session struct {
		CourseID int `json:"course_id" binding:"required"`
	}

	if err := c.ShouldBindJSON(&session); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 生成唯一的streamKey
	streamKey := generateStreamKey()

	// 在数据库中创建直播会话
	result, err := db.Exec(`
		INSERT INTO live_sessions (course_id, stream_key, status, created_at)
		VALUES (?, ?, 'pending', NOW())
	`, session.CourseID, streamKey)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create live session"})
		return
	}

	// 获取新创建的会话ID
	id, err := result.LastInsertId()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get session ID"})
		return
	}

	// 在Livego中创建流
	if err := createStreamInLivego(streamKey); err != nil {
		// 回滚数据库操作
		db.Exec("DELETE FROM live_sessions WHERE id = ?", id)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create stream in Livego"})
		return
	}

	// 返回直播会话信息
	c.JSON(http.StatusCreated, LiveSession{
		ID:        int(id),
		CourseID:  session.CourseID,
		StreamKey: streamKey,
		Status:    "pending",
		CreatedAt: time.Now(),
		PlayURLs:  getPlayURLs(streamKey),
	})
}

// 生成唯一的streamKey
func generateStreamKey() string {
	// 实际项目中应使用更安全的随机生成方法
	return fmt.Sprintf("live_%d_%s", time.Now().Unix(), generateRandomString(10))
}

// 生成随机字符串
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(result)
}

// 在Livego中创建流
func createStreamInLivego(streamKey string) error {
	url := fmt.Sprintf("%s/api/stream/add?stream=%s", config.LivegoURL, streamKey)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to create stream in Livego: %s", resp.Status)
	}

	return nil
}

// 获取播放URLs
func getPlayURLs(streamKey string) map[string]string {
	return map[string]string{
		"rtmp": fmt.Sprintf("rtmp://%s/live/%s", config.LivegoURL, streamKey),
		"flv":  fmt.Sprintf("http://%s:7001/live/%s.flv", config.LivegoURL, streamKey),
		"hls":  fmt.Sprintf("http://%s:7002/live/%s.m3u8", config.LivegoURL, streamKey),
	}
}

// 获取直播会话
func getLiveSession(c *gin.Context) {
	id := c.Param("id")

	var session LiveSession
	err := db.QueryRow(`
		SELECT id, course_id, stream_key, status, start_time, end_time, created_at
		FROM live_sessions
		WHERE id = ?
	`, id).Scan(
		&session.ID,
		&session.CourseID,
		&session.StreamKey,
		&session.Status,
		&session.StartTime,
		&session.EndTime,
		&session.CreatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Live session not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get live session"})
		}
		return
	}

	// 添加播放URLs
	if session.Status == "live" {
		session.PlayURLs = getPlayURLs(session.StreamKey)
	}

	c.JSON(http.StatusOK, session)
}

// 开始直播会话
func startLiveSession(c *gin.Context) {
	id := c.Param("id")

	// 更新数据库状态
	result, err := db.Exec(`
		UPDATE live_sessions
		SET status = 'live', start_time = NOW()
		WHERE id = ? AND status = 'pending'
	`, id)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start live session"})
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check rows affected"})
		return
	}

	if rowsAffected == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Live session not found or already started"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Live session started successfully"})
}

// 结束直播会话
func endLiveSession(c *gin.Context) {
	id := c.Param("id")

	// 更新数据库状态
	result, err := db.Exec(`
		UPDATE live_sessions
		SET status = 'ended', end_time = NOW()
		WHERE id = ? AND status = 'live'
	`, id)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to end live session"})
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check rows affected"})
		return
	}

	if rowsAffected == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Live session not found or already ended"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Live session ended successfully"})
}

// 处理Livego状态回调
func handleLiveStatusCallback(c *gin.Context) {
	var callback struct {
		StreamPath string `json:"streamPath"`
		Status     string `json:"status"`
	}

	if err := c.ShouldBindJSON(&callback); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 从streamPath中提取streamKey
	// 格式通常为 /live/stream_key
	parts := strings.Split(callback.StreamPath, "/")
	if len(parts) < 3 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid stream path"})
		return
	}

	streamKey := parts[2]

	// 更新直播会话状态
	if callback.Status == "start" {
		db.Exec(`
			UPDATE live_sessions
			SET status = 'live', start_time = NOW()
			WHERE stream_key = ? AND status = 'pending'
		`, streamKey)
	} else if callback.Status == "stop" {
		db.Exec(`
			UPDATE live_sessions
			SET status = 'ended', end_time = NOW()
			WHERE stream_key = ? AND status = 'live'
		`, streamKey)
	}

	c.JSON(http.StatusOK, gin.H{"message": "Callback received"})
}

// 创建题目
func createQuestion(c *gin.Context) {
	var question Question
	if err := c.ShouldBindJSON(&question); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 在数据库中创建题目
	result, err := db.Exec(`
		INSERT INTO questions (course_id, type, content, options, answer)
		VALUES (?, ?, ?, ?, ?)
	`, question.CourseID, question.Type, question.Content, strings.Join(question.Options, ","), question.Answer)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create question"})
		return
	}

	// 获取新创建的题目 ID
	id, err := result.LastInsertId()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get question ID"})
		return
	}

	question.ID = int(id)
	c.JSON(http.StatusCreated, question)
}

// 推送题目
func pushQuestion(c *gin.Context) {
	courseID := c.Param("course_id")
	questionID := c.Param("question_id")

	// 获取题目信息
	var question Question
	err := db.QueryRow(`
		SELECT id, course_id, type, content, options, answer
		FROM questions
		WHERE id = ? AND course_id = ?
	`, questionID, courseID).Scan(
		&question.ID,
		&question.CourseID,
		&question.Type,
		&question.Content,
		&question.Options,
		&question.Answer,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Question not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get question"})
		}
		return
	}

	// 推送题目到学生端（使用 WebSocket 或其他实时通信技术）
	// 这里只是简单返回题目信息
	c.JSON(http.StatusOK, question)
}

// 提交答案
func submitAnswer(c *gin.Context) {
	var answer struct {
		QuestionID int    `json:"question_id" binding:"required"`
		StudentID  int    `json:"student_id" binding:"required"`
		Answer     string `json:"answer" binding:"required"`
	}

	if err := c.ShouldBindJSON(&answer); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 在数据库中存储答案
	_, err := db.Exec(`
		INSERT INTO answers (question_id, student_id, answer)
		VALUES (?, ?, ?)
	`, answer.QuestionID, answer.StudentID, answer.Answer)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to submit answer"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Answer submitted successfully"})
}

// 统计结果
func getResult(c *gin.Context) {
	questionID := c.Param("question_id")

	// 获取正确答案
	var correctAnswer string
	err := db.QueryRow(`
		SELECT answer
		FROM questions
		WHERE id = ?
	`, questionID).Scan(&correctAnswer)

	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Question not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get correct answer"})
		}
		return
	}

	// 统计答案
	var totalCount int
	var correctCount int
	err = db.QueryRow(`
		SELECT COUNT(*), SUM(CASE WHEN answer = ? THEN 1 ELSE 0 END)
		FROM answers
		WHERE question_id = ?
	`, correctAnswer, questionID).Scan(&totalCount, &correctCount)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get result"})
		return
	}

	result := map[string]int{
		"total_count":   totalCount,
		"correct_count": correctCount,
	}

	c.JSON(http.StatusOK, result)
}
