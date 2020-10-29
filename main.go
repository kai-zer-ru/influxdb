package influxdb

import (
	"context"
	"time"
	"errors"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)


// InfluxDB struct
type InfluxDB struct {
	isConnected      bool
	client           influxdb2.Client
	writeAPI         api.WriteAPIBlocking
	HostPort         string
	MainDatabaseName string
	Organisation	 string
	Bucket 			 string
	StatStopChannel  chan int
}

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
		opt := influxdb2.DefaultOptions()
		opt.SetLogLevel(3)
		i.client = influxdb2.NewClientWithOptions("http://"+i.HostPort, "", opt)
		i.writeAPI = i.client.WriteAPIBlocking(i.Organisation, i.Bucket)
		i.isConnected = true
		i.StatStopChannel = make(chan int)
	}
	return nil
}

// SendData to influxdb
func (i *InfluxDB) SendData(daemonName, pointName string, value interface{}) error {
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
func (i *InfluxDB) StatHandler(daemonNameForGrafana string) error {
	err := i.SendData(daemonNameForGrafana, "IsRunning", 1)
	if err != nil {
		return err
	}
	for {
		select{
		case <-time.After(time.Second):
			err = i.SendData(daemonNameForGrafana, "IsRunning", 1)
			if err != nil {
				return err
			}
		case <-i.StatStopChannel:
			return nil
		}
	}
}