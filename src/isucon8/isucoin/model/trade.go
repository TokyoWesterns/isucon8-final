package model

import (
	"database/sql"
	"isucon8/isubank"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
)

//go:generate scanner
type Trade struct {
	ID        int64     `json:"id"`
	Amount    int64     `json:"amount"`
	Price     int64     `json:"price"`
	CreatedAt time.Time `json:"created_at"`
}

//go:generate scanner
type CandlestickData struct {
	Time    string `json:"time"`
	TimeSec uint64
	Open    int64 `json:"open"`
	Close   int64 `json:"close"`
	High    int64 `json:"high"`
	Low     int64 `json:"low"`
}

func GetTradeByID(d QueryExecutor, id int64) (*Trade, error) {
	return scanTrade(d.Query("SELECT * FROM trade WHERE id = ?", id))
}

func GetLatestTrade(d QueryExecutor) (*Trade, error) {
	return scanTrade(d.Query("SELECT * FROM trade ORDER BY id DESC LIMIT 1"))
}

var candleStick1Sec []*CandlestickData
var candleStick1Min []*CandlestickData
var candleStick1Hour []*CandlestickData
var candleStickMutex *sync.RWMutex
var candleStickLastIndex int64
var candleStickBaseTime *time.Time

func InitializeCandleStack(baseTime *time.Time) {
	candleStickMutex = &sync.RWMutex{}
	candleStickLastIndex = 0
	candleStick1Sec = make([]*CandlestickData, 0, 1000)
	candleStick1Min = make([]*CandlestickData, 0, 1000)
	candleStick1Hour = make([]*CandlestickData, 0, 1000)
	candleStickBaseTime = baseTime
}

func appendCandleStick(data *[]*CandlestickData, ts uint64, price int64, id int64) {
	if len(*data) == 0 || (*data)[len(*data)-1].TimeSec != ts {
		// Create
		target := &CandlestickData{}
		target.High = price
		target.Low = price
		target.Close = price
		target.Open = price
		target.TimeSec = ts
		target.Time = time.Unix(int64(ts), 0).Format("2006-01-02T15:04:05+09:00")
		*data = append(*data, target)
	} else {
		// Update
		target := (*data)[len(*data)-1]
		target.Close = price
		if target.High < price {
			target.High = price
		}
		if target.Low > price {
			target.Low = price
		}
	}
}

func UpdateCandleStickData(d QueryExecutor) error {
	candleStickMutex.Lock()
	defer candleStickMutex.Unlock()
	query := `
			SELECT
				UNIX_TIMESTAMP(STR_TO_DATE(DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s'), '%Y-%m-%d %H:%i:%s')) as t,
				price,
				id
			FROM trade
			WHERE created_at >= ? AND id > ?
			ORDER BY id
	`
	rows, err := d.Query(query, candleStickBaseTime.Add(-48*time.Hour), candleStickLastIndex)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var ts uint64
		var price int64
		var id int64
		if err = rows.Scan(&ts, &price, &id); err != nil {
			return err
		}
		// Update
		appendCandleStick(&candleStick1Sec, ts, price, id)
		appendCandleStick(&candleStick1Min, ts/60*60, price, id)
		appendCandleStick(&candleStick1Hour, ts/3600*3600, price, id)
		candleStickLastIndex = id
	}
	return nil
}

func getCandleStick(data []*CandlestickData, ts uint64) []*CandlestickData {
	candleStickMutex.RLock()
	defer candleStickMutex.RUnlock()
	low := 0
	high := len(data)
	// data.TimeSec >= tsのデータを取得する
	for low+1 < high {
		mid := (low + high) / 2
		if data[mid].TimeSec < ts {
			low = mid
		} else {
			high = mid
		}
	}
	return append(make([]*CandlestickData, 0), data[low:]...)
}

func GetCandlestick1Sec(mt time.Time) []*CandlestickData {
	return getCandleStick(candleStick1Sec, uint64(mt.Unix()))
}
func GetCandlestick1Min(mt time.Time) []*CandlestickData {
	return getCandleStick(candleStick1Min, uint64(mt.Unix()))
}
func GetCandlestick1Hour(mt time.Time) []*CandlestickData {
	return getCandleStick(candleStick1Hour, uint64(mt.Unix()))
}
func GetCandlestickData(d QueryExecutor, mt time.Time, tf string) ([]*CandlestickData, error) {
	if err := UpdateCandleStickData(d); err != nil {
		return nil, err
	}
	return nil, nil
}

func HasTradeChanceByOrder(d QueryExecutor, orderID int64) (bool, error) {
	order, err := GetOrderByID(d, orderID)
	if err != nil {
		return false, err
	}

	lowest, err := GetLowestSellOrder(d)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, errors.Wrap(err, "GetLowestSellOrder")
	}

	highest, err := GetHighestBuyOrder(d)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, errors.Wrap(err, "GetHighestBuyOrder")
	}

	switch order.Type {
	case OrderTypeBuy:
		if lowest.Price <= order.Price {
			return true, nil
		}
	case OrderTypeSell:
		if order.Price <= highest.Price {
			return true, nil
		}
	default:
		return false, errors.Errorf("other type [%s]", order.Type)
	}
	return false, nil
}

func reserveOrder(d QueryExecutor, order *Order, price int64) (int64, error) {
	bank, err := Isubank(d)
	if err != nil {
		return 0, errors.Wrap(err, "isubank init failed")
	}
	p := order.Amount * price
	if order.Type == OrderTypeBuy {
		p *= -1
	}

	id, err := bank.Reserve(order.User.BankID, p)
	if err != nil {
		if err == isubank.ErrCreditInsufficient {
			if derr := cancelOrder(d, order, "reserve_failed"); derr != nil {
				return 0, derr
			}
			sendLog(d, order.Type+".error", map[string]interface{}{
				"error":   err.Error(),
				"user_id": order.UserID,
				"amount":  order.Amount,
				"price":   price,
			})
			return 0, err
		}
		return 0, errors.Wrap(err, "isubank.Reserve")
	}

	return id, nil
}

func commitReservedOrder(tx *sql.Tx, order *Order, targets []*Order, reserves []int64) error {
	res, err := tx.Exec(`INSERT INTO trade (amount, price, created_at) VALUES (?, ?, NOW(6))`, order.Amount, order.Price)
	if err != nil {
		return errors.Wrap(err, "insert trade")
	}
	tradeID, err := res.LastInsertId()
	if err != nil {
		return errors.Wrap(err, "lastInsertID for trade")
	}
	sendLog(tx, "trade", map[string]interface{}{
		"trade_id": tradeID,
		"price":    order.Price,
		"amount":   order.Amount,
	})
	for _, o := range append(targets, order) {
		if _, err = tx.Exec(`UPDATE orders SET trade_id = ?, closed_at = NOW(6) WHERE id = ?`, tradeID, o.ID); err != nil {
			return errors.Wrap(err, "update order for trade")
		}
		sendLog(tx, o.Type+".trade", map[string]interface{}{
			"order_id": o.ID,
			"price":    order.Price,
			"amount":   o.Amount,
			"user_id":  o.UserID,
			"trade_id": tradeID,
		})
	}
	bank, err := Isubank(tx)
	if err != nil {
		return errors.Wrap(err, "isubank init failed")
	}
	if err = bank.Commit(reserves); err != nil {
		return errors.Wrap(err, "commit")
	}
	return nil
}

func tryTrade(tx *sql.Tx, orderID int64) error {
	order, err := getOpenOrderByID(tx, orderID)
	if err != nil {
		//log.Println("order target", orderID)
		return err
	}

	restAmount := order.Amount
	unitPrice := order.Price
	reserves := make([]int64, 1, order.Amount+1)
	targets := make([]*Order, 0, order.Amount)

	reserves[0], err = reserveOrder(tx, order, unitPrice)
	if err != nil {
		return err
	}
	defer func() {
		if len(reserves) > 0 {
			bank, err := Isubank(tx)
			if err != nil {
				log.Printf("[WARN] isubank init failed. err:%s", err)
				return
			}
			if err = bank.Cancel(reserves); err != nil {
				log.Printf("[WARN] isubank cancel failed. err:%s", err)
			}
		}
	}()

	var targetOrders []*Order
	switch order.Type {
	case OrderTypeBuy:
		targetOrders, err = scanOrders(tx.Query(`SELECT * FROM orders WHERE type = ? AND closed_at IS NULL AND price <= ? ORDER BY price ASC, created_at ASC, id ASC`, OrderTypeSell, order.Price))
	case OrderTypeSell:
		targetOrders, err = scanOrders(tx.Query(`SELECT * FROM orders WHERE type = ? AND closed_at IS NULL AND price >= ? ORDER BY price DESC, created_at ASC, id ASC`, OrderTypeBuy, order.Price))
	}
	if err != nil {
		return errors.Wrap(err, "find target orders")
	}
	if len(targetOrders) == 0 {
		return ErrNoOrderForTrade
	}

	for _, to := range targetOrders {
		to, err = getOpenOrderByID(tx, to.ID)
		if err != nil {
			if err == ErrOrderAlreadyClosed {
				continue
			}
			log.Println("TargetOrder", orderID)
			return errors.Wrap(err, "getOpenOrderByID  buy_order")
		}
		if to.Amount > restAmount {
			continue
		}
		rid, err := reserveOrder(tx, to, unitPrice)
		if err != nil {
			if err == isubank.ErrCreditInsufficient {
				continue
			}
			return err
		}
		reserves = append(reserves, rid)
		targets = append(targets, to)
		restAmount -= to.Amount
		if restAmount == 0 {
			break
		}
	}
	if restAmount > 0 {
		return ErrNoOrderForTrade
	}
	if err = commitReservedOrder(tx, order, targets, reserves); err != nil {
		return err
	}
	reserves = reserves[:0]
	return nil
}

func RunTrade(db *sql.DB) error {
	lowestSellOrder, err := GetLowestSellOrder(db)
	switch {
	case err == sql.ErrNoRows:
		// 売り注文が無いため成立しない
		return nil
	case err != nil:
		return errors.Wrap(err, "GetLowestSellOrder")
	}

	highestBuyOrder, err := GetHighestBuyOrder(db)
	switch {
	case err == sql.ErrNoRows:
		// 買い注文が無いため成立しない
		return nil
	case err != nil:
		return errors.Wrap(err, "GetHighestBuyOrder")
	}

	if lowestSellOrder.Price > highestBuyOrder.Price {
		// 最安の売値が最高の買値よりも高いため成立しない
		return nil
	}

	candidates := make([]int64, 0, 2)
	if lowestSellOrder.Amount > highestBuyOrder.Amount {
		candidates = append(candidates, lowestSellOrder.ID, highestBuyOrder.ID)
	} else {
		candidates = append(candidates, highestBuyOrder.ID, lowestSellOrder.ID)
	}

	for _, orderID := range candidates {
		err := func() error {
			tx, err := db.Begin()
			if err != nil {
				return errors.Wrap(err, "begin transaction failed")
			}
			err = tryTrade(tx, orderID)
			switch err {
			case nil, ErrNoOrderForTrade, ErrOrderAlreadyClosed, isubank.ErrCreditInsufficient:
				tx.Commit()
			default:
				tx.Rollback()
			}
			return err
		}()
		switch err {
		case nil:
			// トレード成立したため次の取引を行う
			return RunTrade(db)
		case ErrNoOrderForTrade, ErrOrderAlreadyClosed:
			// 注文個数の多い方で成立しなかったので少ない方で試す
			continue
		default:
			return err
		}
	}
	// 個数のが不足していて不成立
	return nil
}
