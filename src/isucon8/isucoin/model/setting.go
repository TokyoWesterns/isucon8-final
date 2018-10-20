package model

import (
	"isucon8/isubank"
	"isucon8/isulogger"
	"log"

	"github.com/pkg/errors"
)

const (
	BankEndpoint = "bank_endpoint"
	BankAppid    = "bank_appid"
	LogEndpoint  = "log_endpoint"
	LogAppid     = "log_appid"
)

//go:generate scanner
type Setting struct {
	Name string
	Val  string
}

var bankEndpoint string
var bankAppId string
var logEndpoint string
var logAppId string

func SetSetting(d QueryExecutor, k, v string) error {
	_, err := d.Exec(`INSERT INTO setting (name, val) VALUES (?, ?) ON DUPLICATE KEY UPDATE val = VALUES(val)`, k, v)
	bankEndpoint = ""
	bankAppId = ""
	logEndpoint = ""
	logAppId = ""
	return err
}

func GetSetting(d QueryExecutor, k string, memo *string) (string, error) {
	if *memo != "" {
		return *memo, nil
	}

	s, err := scanSetting(d.Query(`SELECT * FROM setting WHERE name = ?`, k))
	if err != nil {
		return "", err
	}
	*memo = s.Val
	return s.Val, nil
}

func Isubank(d QueryExecutor) (*isubank.Isubank, error) {
	ep, err := GetSetting(d, BankEndpoint, &bankEndpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "getSetting failed. %s", BankEndpoint)
	}
	id, err := GetSetting(d, BankAppid, &bankAppId)
	if err != nil {
		return nil, errors.Wrapf(err, "getSetting failed. %s", BankAppid)
	}
	return isubank.NewIsubank(ep, id)
}

func Logger(d QueryExecutor) (*isulogger.Isulogger, error) {
	ep, err := GetSetting(d, LogEndpoint, &logEndpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "getSetting failed. %s", LogEndpoint)
	}
	id, err := GetSetting(d, LogAppid, &logAppId)
	if err != nil {
		return nil, errors.Wrapf(err, "getSetting failed. %s", LogAppid)
	}
	return isulogger.NewIsulogger(ep, id)
}

func sendLog(d QueryExecutor, tag string, v interface{}) {
	logger, err := Logger(d)
	if err != nil {
		log.Printf("[WARN] new logger failed. tag: %s, v: %v, err:%s", tag, v, err)
		return
	}
	err = logger.Send(tag, v)
	if err != nil {
		log.Printf("[WARN] logger send failed. tag: %s, v: %v, err:%s", tag, v, err)
	}
}
