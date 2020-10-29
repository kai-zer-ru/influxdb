package influxdb

import (
	"context"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// InfluxDB struct
type InfluxDB struct {
	HostPort         string
	isConnected      bool
	client           influxdb2.Client
	writeAPI         api.WriteAPIBlocking
	MainDatabaseName string
}

// Connect to influxdb
func (i *InfluxDB) Connect() {
	if !i.isConnected {
		i.client = influxdb2.NewClient("http://"+i.HostPort, "")
		i.writeAPI = i.client.WriteAPIBlocking("", "")
		i.isConnected = true
	}
}

// SendData to influxdb
func (i *InfluxDB) SendData(daemonName, pointName string, value interface{}) {
	if !i.isConnected {
		return
	}
	p := influxdb2.NewPoint(i.MainDatabaseName,
		map[string]string{daemonName: pointName},
		map[string]interface{}{"value": value},
		time.Now(),
	)
	i.writeAPI.WritePoint(context.Background(), p)
}

// Close influxdb
func (i *InfluxDB) Close() {
	if !i.isConnected {
		return
	}
	i.client.Close()
	i.isConnected = false
}
