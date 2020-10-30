package influxdb

import (
	"context"
	"errors"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"sync"
	"time"
)

// InfluxDB struct
type InfluxDB struct {
	isConnected      bool
	client           influxdb2.Client
	writeAPI         api.WriteAPIBlocking
	HostPort         string
	MainDatabaseName string
	Organisation     string
	Bucket           string
	StatStopChannel  chan int
	SaveSecondPeriod int64
}

type statData struct {
	statType string
	counter  int64
}

var (
	statChannels     = make(chan statData, 1000000) // канал сбора статистики
	lockStat         sync.Mutex
	statCounters     = make(map[string]int64) // данные по счетчикам
)

// Connect to influxdb
func (i *InfluxDB) Connect() error {
	if !i.isConnected {
		if i.Bucket == "" {
			return errors.New("no bucket name")
		}
		if i.Organisation == "" {
			return errors.New("no org name")
		}
		if i.HostPort == "" {
			return errors.New("no host name")
		}
		if i.MainDatabaseName == "" {
			return errors.New("no database name")
		}
		if i.SaveSecondPeriod == 0 {
			i.SaveSecondPeriod = 60
		}
		opt := influxdb2.DefaultOptions()
		opt.SetLogLevel(3)
		i.client = influxdb2.NewClientWithOptions("http://"+i.HostPort, "", opt)
		i.writeAPI = i.client.WriteAPIBlocking(i.Organisation, i.Bucket)
		i.isConnected = true
		i.StatStopChannel = make(chan int)
	}
	return nil
}

func (i *InfluxDB) sendData(daemonName, pointName string, value interface{}) error {
	if !i.isConnected {
		return errors.New("not connected")
	}
	p := influxdb2.NewPoint(i.MainDatabaseName,
		map[string]string{daemonName: pointName},
		map[string]interface{}{"value": value},
		time.Now(),
	)
	return i.writeAPI.WritePoint(context.Background(), p)
}

// Close influxdb
func (i *InfluxDB) Close() {
	if !i.isConnected {
		return
	}
	i.client.Close()
	i.isConnected = false
}

// StatHandler isRunning
func (i *InfluxDB) StatHandler(daemonNameForGrafana string) {
	err := i.sendData(daemonNameForGrafana, "IsRunning", 1)
	if err != nil {
		return
	}
	for {
		select {
		case <-time.After(time.Duration(i.SaveSecondPeriod) * time.Second):
			lockStat.Lock()
			_ = i.sendData(daemonNameForGrafana, "IsRunning", 1)
			for k, v := range statCounters {
				_ = i.sendData(daemonNameForGrafana, k, v)
			}
			statCounters = map[string]int64{}
			lockStat.Unlock()
		case <-i.StatStopChannel:
			return
		}
	}
}

// SendValueStatData function
func (i *InfluxDB) SendValueStatData(statType string, value int64) {
	lockStat.Lock()
	d, ok := statCounters[statType]
	if !ok {
		statCounters[statType] = value
	} else {
		statCounters[statType] = d + value
	}
	lockStat.Unlock()
}
