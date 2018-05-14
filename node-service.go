/*
Copyright 2018 Idealnaya rabota LLC
Licensed under Multy.io license.
See LICENSE for details
*/
package node

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"sync"

	"github.com/Appscrunch/Multy-BTC-node-service/btc"
	"github.com/Appscrunch/Multy-BTC-node-service/streamer"
	pb "github.com/Appscrunch/Multy-back/node-streamer/btc"
	"github.com/Appscrunch/Multy-back/store"
	"github.com/KristinaEtc/slf"
	"github.com/blockcypher/gobcy"
	"google.golang.org/grpc"
)

var (
	log = slf.WithContext("NodeClient")
)

// Multy is a main struct of service

// Client is a main struct of service
type Client struct {
	Config     *Configuration
	Instance   *btc.Client
	GRPCserver *streamer.Server
	Clients    *map[string]store.AddressExtended // address to userid
	BTCApi     *gobcy.API
}

// Init initializes Multy instance
func Init(conf *Configuration) (*Client, error) {
	cli := &Client{
		Config: conf,
	}

	var usersData = map[string]store.AddressExtended{
		"2MvPhdUf3cwaadRKsSgbQ2SXc83CPcBJezT": store.AddressExtended{
			UserID:       "kek",
			WalletIndex:  1,
			AddressIndex: 2,
		},
	}

	api := gobcy.API{
		Token: conf.BTCAPI.Token,
		Coin:  conf.BTCAPI.Coin,
		Chain: conf.BTCAPI.Chain,
	}
	cli.BTCApi = &api
	log.Debug("btc api initialization done √")

	// initail initialization of clients data
	cli.Clients = &usersData
	log.Debug("Users data initialization done √")

	// init gRPC server
	lis, err := net.Listen("tcp", conf.GrpcPort)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %v", err.Error())
	}

	usersDataM := &sync.Mutex{}
	rpcClientM := &sync.Mutex{}

	btcClient, err := btc.NewClient(getCertificate(conf.BTCSertificate), conf.BTCNodeAddress, cli.Clients, usersDataM, rpcClientM)
	if err != nil {
		return nil, fmt.Errorf("Blockchain api initialization: %s", err.Error())
	}
	log.Debug("BTC client initialization done √")
	cli.Instance = btcClient

	// Creates a new gRPC server
	s := grpc.NewServer()
	srv := streamer.Server{
		UsersData: cli.Clients,
		BTCApi:    cli.BTCApi,
		M:         &sync.Mutex{},
		BTCCli:    btcClient,
		Info:      &conf.ServiceInfo,
	}

	pb.RegisterNodeCommuunicationsServer(s, &srv)
	go s.Serve(lis)
	log.Debug("NodeCommuunications Server initialization done √")

	return cli, nil
}

func getCertificate(certFile string) []byte {
	cert, err := ioutil.ReadFile(certFile)
	cert = bytes.Trim(cert, "\x00")

	if err != nil {
		log.Errorf("get certificate: %s", err.Error())
		return []byte{}
	}
	if len(cert) > 1 {
		return cert
	}
	log.Errorf("get certificate: empty certificate")
	return []byte{}
}
