package vksnitch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"context"
)

func createOneHostTransport() *http.Transport {
	result := http.DefaultTransport.(*http.Transport).Clone()

	result.MaxIdleConnsPerHost = result.MaxIdleConns

	return result
}

const (
	DefaultQueueSize = 15
	ApiEventEndpoint = "https://tracker-s2s.my.com/v1/customEventBatch/?idApp=%s"
	ApiAuthEndpoint  = "https://tracker-s2s.my.com/v1/userInfo/?idApp=%s"
)

type Event struct {
	UserId          uint      `json:"-"`
	CustomUserId    string    `json:"customUserId"`
	Time            time.Time `json:"-"`
	TimeInMillis    int64     `json:"eventTimestamp"`
	CustomEventName string    `json:"customEventName"`
}

type AuthEvent struct {
	CustomUserId string `json:"customUserId"`
	TimeInMillis int64  `json:"eventTimestamp"`
	VkIds        []uint `json:"vkIds"`
}

type Client struct {
	cancel           func()
	ctx              context.Context
	appId            string
	authToken        string
	ch               chan Event
	flush            chan chan struct{}
	queueSize        int
	interval         time.Duration
	onPublishFunc    func(status int, err error)
	apiEventEndpoint string
	apiAuthEndpoint  string
	httpClient       *http.Client
}

func New(appId string, authToken string, options ...Option) *Client {
	ctx, cancel := context.WithCancel(context.Background())

	client := &Client{
		cancel:           cancel,
		ctx:              ctx,
		appId:            appId,
		authToken:        authToken,
		ch:               make(chan Event, DefaultQueueSize),
		flush:            make(chan chan struct{}),
		queueSize:        DefaultQueueSize,
		interval:         time.Second * 15,
		onPublishFunc:    func(status int, err error) {},
		apiEventEndpoint: fmt.Sprintf(ApiEventEndpoint, appId),
		apiAuthEndpoint:  fmt.Sprintf(ApiAuthEndpoint, appId),
	}

	client.httpClient = &http.Client{}
	client.httpClient.Transport = createOneHostTransport()

	for _, opt := range options {
		opt(client)
	}

	go client.start()

	return client
}

func (c *Client) Publish(e Event) error {
	if !e.Time.IsZero() {
		e.TimeInMillis = e.Time.UnixNano() / int64(time.Millisecond)
	}

	e.CustomUserId = fmt.Sprintf("vkid:%d", e.UserId)

	select {
	case c.ch <- e:
		return nil
	default:
		return fmt.Errorf("Unable to send event, queue is full.  Use a larger queue size or create more workers.")
	}
}

func (c *Client) start() {
	timer := time.NewTimer(c.interval)

	bufferSize := 20
	buffer := make([]Event, bufferSize)
	index := 0

	for {
		timer.Reset(c.interval)

		select {
		case <-c.ctx.Done():
			return

		case <-timer.C:
			if index > 0 {
				c.publish(buffer[0:index])
				index = 0
			}

		case v := <-c.ch:
			buffer[index] = v
			index++
			if index == bufferSize {
				c.publish(buffer[0:index])
				index = 0
			}

		case v := <-c.flush:
			if index > 0 {
				c.publish(buffer[0:index])
				index = 0
			}
			v <- struct{}{}
		}
	}
}

func (c *Client) publish(events []Event) error {
	data, err := json.Marshal(events)
	if err != nil {
		return err
	}

	r := bytes.NewReader(data)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)

	req, err := http.NewRequest("POST", c.apiEventEndpoint, r)
	req.Header.Set("Authorization", c.authToken)
	req.WithContext(ctx)

	resp, err := c.httpClient.Do(req)

	if resp != nil {
		defer resp.Body.Close()
	}
	c.onPublishFunc(resp.StatusCode, err)

	return err
}

func (c *Client) publishAuth(event AuthEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	r := bytes.NewReader(data)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)

	req, err := http.NewRequest("POST", c.apiAuthEndpoint, r)
	req.Header.Set("Authorization", c.authToken)
	req.WithContext(ctx)

	resp, err := c.httpClient.Do(req)

	if resp != nil {
		defer resp.Body.Close()
	}

	return err
}

func (c *Client) Auth(userId uint) {

	var e AuthEvent
	e.TimeInMillis = time.Now().Unix()

	e.CustomUserId = fmt.Sprintf("vkid:%d", userId)
	e.VkIds = []uint{
		userId,
	}

	go c.publishAuth(e)
}

func (c *Client) Flush() {
	ch := make(chan struct{})
	defer close(ch)

	c.flush <- ch
	<-ch
}

func (c *Client) Close() {
	c.Flush()
	c.cancel()
}
