// Package isulogger is client for ISULOG
package isulogger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"
)

// Log はIsuloggerに送るためのログフォーマット
type Log struct {
	// Tagは各ログを識別するための情報です
	Tag string `json:"tag"`
	// Timeはログの発生時間
	Time time.Time `json:"time"`
	// Data はログの詳細情報でTagごとに決められています
	Data interface{} `json:"data"`
}

type Isulogger struct {
	endpoint *url.URL
	appID    string
	AppId    string
	EndPoint string
	logChan  chan *Log
}

var Logger *Isulogger

// NewIsulogger はIsuloggerを初期化します
//
// endpoint: ISULOGを利用するためのエンドポイントURI
// appID:    ISULOGを利用するためのアプリケーションID
func NewIsulogger(endpoint, appID string) (*Isulogger, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	logger := &Isulogger{
		endpoint: u,
		appID:    appID,
		AppId:    appID,
		EndPoint: endpoint,
		logChan:  make(chan *Log, 10000),
	}
	go logger.process(logger.logChan)
	return logger, nil
}

// Send はログを送信します
func (b *Isulogger) Send(tag string, data interface{}) error {
	b.logChan <- &Log{
		Tag:  tag,
		Time: time.Now(),
		Data: data,
	}
	return nil
}

func (b *Isulogger) request(p string, v interface{}) error {
	u := new(url.URL)
	*u = *b.endpoint
	u.Path = path.Join(u.Path, p)

	body := &bytes.Buffer{}
	if err := json.NewEncoder(body).Encode(v); err != nil {
		return fmt.Errorf("logger json encode failed. err: %s", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), body)
	if err != nil {
		return fmt.Errorf("logger new request failed. err: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+b.appID)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("logger request failed. err: %s", err)
	}
	defer res.Body.Close()
	bo, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("logger body read failed. err: %s", err)
	}
	if res.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("logger status is not ok. code: %d, body: %s", res.StatusCode, string(bo))
}

func (b *Isulogger) requestBluk(p *bytes.Buffer) error {
	u := new(url.URL)
	*u = *b.endpoint
	u.Path = path.Join(u.Path, "send_bulk")

	req, err := http.NewRequest(http.MethodPost, u.String(), p)
	if err != nil {
		return fmt.Errorf("logger new request failed. err: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+b.appID)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("logger request failed. err: %s", err)
	}
	defer res.Body.Close()
	bo, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("logger body read failed. err: %s", err)
	}
	if res.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("logger status is not ok. code: %d, body: %s", res.StatusCode, string(bo))
}

func (b *Isulogger) process(logChan chan *Log) {
	t := time.NewTicker(5 * time.Second)
	r := bytes.NewBuffer(make([]byte, 0))
	r.WriteString("[")
	first := true
	for {
		select {
		case <-t.C:
			// FlushBuffer
			if first {
				continue
			}
			r.WriteString("]")
			if err := b.requestBluk(r); err != nil {
				fmt.Println(err)
			}

			first = true
			r = bytes.NewBuffer(make([]byte, 0))
			r.WriteString("[")
		case log := <-logChan:
			// logの追加
			j, err := json.Marshal(log)
			if err != nil {
				fmt.Errorf("logger: Failed to marshal json\n")
				continue
			}
			if r.Len()+len(j) >= 10*100*1000-1 {
				// 9KBに到達したらFlushする
				r.WriteString("]")
				if err := b.requestBluk(r); err != nil {
					fmt.Println(err)
				}

				first = true
				r = bytes.NewBuffer(make([]byte, 1024*1024))
				r.WriteString("[")
			}
			if !first {
				r.WriteString(",")
			}
			r.Write(j)
			first = false
		}
	}
}
