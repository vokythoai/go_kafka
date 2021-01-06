package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Message struct {
	ID              int32                  `json:"id"`
	UID             string                 `json:"uid"`
	Topic           string                 `json:"topic"`
	Source          string                 `json:"source"`
	Referrer        string                 `json:"referrer"`
	Aud             string                 `json:"aud"`
	Event           string                 `json:"event"`
	Payload         map[string]interface{} `json:"payload"`
	StreamType      string                 `json:"stream_type"`
	StreamID        string                 `json:"stream_id"`
	ObjectVersion   int32                  `json:"object_version"`
	ScheduleAt      int64                  `json:"schedule_at"`
	SentAt          int64                  `json:"sent_at"`
	Balancer        int64                  `json:"balancer"`
	Status          int32                  `json:"status"`
	DurationTimeout int32                  `json:"duration_timeout"`
	RetryTimes      int16                  `json:"retry_times"`
	UnconfirmAud    string                 `json:"unconfirm_aud"`
	ParentUID       string                 `json:"parent_uid"`
	CreatedAt       int64                  `json:"created_at"`
	UpdatedAt       int64                  `json:"updated_at"`
	StreamTypeGroup string                 `json:"stream_type_group"`
	ConfirmPayload  map[string]interface{} `json:"confirm_payload"`
	ParentCreatedAt int64                  `json:"parent_created_at"`
}

var hosts = os.Getenv("MONGO_DATABASE_HOST")
var database = os.Getenv("MONGO_INITDB_DATABASE")
var username = os.Getenv("MONGO_INITDB_ROOT_USERNAME")
var password = os.Getenv("MONGO_INITDB_ROOT_PASSWORD")
var collection = "inventory_events"
var poollimit, _ = strconv.Atoi(os.Getenv("MONGO_MAX_POOL_SIZE"))
var clientID = os.Getenv("KARAFKA_CLIENT_ID")
var topics = os.Getenv("KARAFKA_TOPICS")

type Event struct {
	Event_Type int32
	Payload    map[string]interface{}
	Store_ID   string
	Status     int32
	Extra_Data map[string]interface{}
	Updated_At time.Time
	Created_At time.Time
}

type MessageInventoryProduct struct {
	Payload Payload `json:"payload"`
}

type InventoryProduct struct {
	Sold               int32  `json:"sold"`
	Carry              int32  `json:"carry"`
	Ordered            int32  `json:"ordered"`
	ProductID          string `json:"product_id"`
	StoreCode          string `json:"store_code"`
	ProductName        string `json:"product_name"`
	CurrentInventory   int32  `json:"current_inventory"`
	RetailBusinessType string `json:"retail_business_type"`
}

type Payload struct {
	StoreCode         string             `json:"store_code"`
	StoreName         string             `json:"store_name"`
	InventoryProducts []InventoryProduct `json:"inventory_products"`
}
type MongoStore struct {
	session *mgo.Session
}

var mongoStore = MongoStore{}

func initMongo() (session *mgo.Session) {

	info := &mgo.DialInfo{
		Addrs:     []string{hosts},
		Timeout:   60 * time.Second,
		Database:  database,
		Username:  username,
		Password:  password,
		PoolLimit: poollimit,
	}

	session, err := mgo.DialWithInfo(info)
	if err != nil {
		panic(err)
	}

	return

}

func init() {
	sarama.Logger = log.New(os.Stdout, "[OrderDeliveryConsumer] ", log.LstdFlags)
}

var (
	brokers   = flag.String("brokers", strings.Replace(os.Getenv("KARAFKA_BROKERS"), "kafka+ssl://", "", 3), "The Kafka brokers to connect to, as a comma separated list")
	certFile  = flag.String("certificate", "ssl/kk_sondoan.crt", "The optional certificate file for client authentication")
	keyFile   = flag.String("key", "ssl/kk_sondoan.key", "The optional key file for client authentication")
	caFile    = flag.String("ca", "ssl/kafka_server.cer.pem", "The optional certificate authority file for TLS client authentication")
	verifySSL = flag.Bool("verify", true, "Optional verify ssl certificates chain")
	useTLS    = flag.Bool("tls", true, "Use TLS to communicate with the cluster")
	logMsg    = flag.Bool("logmsg", true, "True to log consumed messages to console")
)

func createTLSConfiguration() (t *tls.Config) {
	t = &tls.Config{
		InsecureSkipVerify: *verifySSL,
	}
	if *certFile != "" && *keyFile != "" && *caFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			fmt.Println(err)
		}
		t.Certificates = []tls.Certificate{cert}

		// Load CA cert
		caCert, err := ioutil.ReadFile(*caFile)
		if err != nil {
			fmt.Println(err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		t.RootCAs = caCertPool

		t.BuildNameToCertificate()
		t.InsecureSkipVerify = *verifySSL
	}
	return t
}

func main() {

	flag.Parse()

	if *brokers == "" {
		log.Fatalln("at least one broker is required")
	}
	splitBrokers := strings.Split(*brokers, ",")
	conf := sarama.NewConfig()
	conf.ClientID = clientID
	conf.Producer.Retry.Max = 1
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Metadata.Full = true
	conf.Version = sarama.V0_10_2_0
	conf.Metadata.Full = true
	conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	conf.Consumer.Offsets.Initial = sarama.OffsetNewest

	if *useTLS {
		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = createTLSConfiguration()
	}

	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(splitBrokers, fmt.Sprintf("trigger_group_%s", clientID), conf)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}

}

type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Printf("\nMessage claimed: value = %s, timestamp = %v, topic = %s, partition = %d, offset = %d", string(message.Value), message.Timestamp, message.Topic, message.Partition, message.Offset)

		saveEventToMongo(message.Value)
		session.MarkMessage(message, "")
	}

	return nil
}

func saveEventToMongo(message []byte) {

	//Save data into Job struct
	var _message Message
	var _event Event
	err := json.Unmarshal(message, &_message)
	if err != nil {
		fmt.Println(err)
	}

	if _message.StreamType != "PRODUCT_MAPPING" && _message.StreamType != "CURRENT_INVENTORY_PRODUCT" && _message.StreamType != "INVENTORY_PRODUCT" {
		return
	}

	var eventType int32
	switch _message.StreamType {
	case "PRODUCT_MAPPING":
		eventType = 2
	case "CURRENT_INVENTORY_PRODUCT":
		eventType = 1
	default:
		eventType = 1
	}

	_event.Event_Type = eventType
	_event.Payload = _message.Payload
	_event.Status = 1
	_event.Store_ID = _message.Aud
	_event.Created_At = time.Now()
	_event.Updated_At = time.Now()

	session := initMongo()
	col := session.DB(database)

	if _message.StreamType == "PRODUCT_MAPPING" {
		//Insert job into MongoDB
		errMongo := col.C("inventory_events").Insert(_event)

		if errMongo != nil {
			panic(errMongo)
		} else {
			fmt.Println("Saved to MongoDB")
		}

	} else {
		updateInventoryProduct(message, col)
	}

	defer session.Close()
}

func updateInventoryProduct(event []byte, session *mgo.Database) {
	client := session.C("inventory_products")

	inventoryUpdateMessage := MessageInventoryProduct{}

	err := json.Unmarshal(event, &inventoryUpdateMessage)
	if err != nil {
		fmt.Println(err)
	}

	for _, inventoryProduct := range inventoryUpdateMessage.Payload.InventoryProducts {
		_, error := client.Upsert(bson.M{"product_code": inventoryProduct.ProductID, "store_id": inventoryUpdateMessage.Payload.StoreCode}, bson.M{"$set": bson.M{
			"store_id":         inventoryUpdateMessage.Payload.StoreCode,
			"store_name":       inventoryUpdateMessage.Payload.StoreName,
			"carry":            inventoryProduct.Carry,
			"current_quantity": inventoryProduct.CurrentInventory,
			"product_name":     inventoryProduct.ProductName,
			"product_code":     inventoryProduct.ProductID,
		}})

		if error != nil {
			fmt.Println(error)
		} else {
			fmt.Println("Inventory Product updated.")
		}
	}
}
