package streamer

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Appscrunch/Multy-BTC-node-service/btc"
	pb "github.com/Appscrunch/Multy-back/node-streamer/btc"
	"github.com/Appscrunch/Multy-back/store"
	"github.com/KristinaEtc/slf"
	"github.com/blockcypher/gobcy"
)

var log = slf.WithContext("streamer")

// Server implements streamer interface and is a gRPC server
type Server struct {
	UsersData *map[string]store.AddressExtended
	BTCApi    *gobcy.API
	BTCCli    *btc.Client
	M         *sync.Mutex
	Info      *store.ServiceInfo
}

// ServiceInfo is a method for getting service info
func (s *Server) ServiceInfo(c context.Context, in *pb.Empty) (*pb.ServiceVersion, error) {
	return &pb.ServiceVersion{
		Branch:    s.Info.Branch,
		Commit:    s.Info.Commit,
		Buildtime: s.Info.Buildtime,
		Lasttag:   "",
	}, nil
}

// EventInitialAdd us used to add initial pairs of watch addresses
func (s *Server) EventInitialAdd(c context.Context, ud *pb.UsersData) (*pb.ReplyInfo, error) {
	log.Debugf("EventInitialAdd - %v", ud.Map)

	udMap := map[string]store.AddressExtended{}

	for addr, ex := range ud.GetMap() {
		udMap[addr] = store.AddressExtended{
			UserID:       ex.GetUserID(),
			WalletIndex:  int(ex.GetWalletIndex()),
			AddressIndex: int(ex.GetAddressIndex()),
		}
	}
	s.BTCCli.UserDataM.Lock()
	*s.UsersData = udMap
	s.BTCCli.UserDataM.Unlock()

	return &pb.ReplyInfo{
		Message: "ok",
	}, nil
}

// EventAddNewAddress us used to add new watch address to existing pairs
func (s *Server) EventAddNewAddress(c context.Context, wa *pb.WatchAddress) (*pb.ReplyInfo, error) {
	s.BTCCli.UserDataM.Lock()
	defer s.BTCCli.UserDataM.Unlock()
	newMap := *s.UsersData
	if newMap == nil {
		newMap = map[string]store.AddressExtended{}
	}
	// TODO: binded address fix
	_, ok := newMap[wa.Address]
	if ok {
		return &pb.ReplyInfo{
			Message: "err: Address already binded",
		}, nil
	}
	newMap[wa.Address] = store.AddressExtended{
		UserID:       wa.UserID,
		WalletIndex:  int(wa.WalletIndex),
		AddressIndex: int(wa.AddressIndex),
	}
	*s.UsersData = newMap

	return &pb.ReplyInfo{
		Message: "ok",
	}, nil

}

// EventGetBlockHeight returns blockheight
func (s *Server) EventGetBlockHeight(ctx context.Context, in *pb.Empty) (*pb.BlockHeight, error) {
	h, err := s.BTCCli.RPCClient.GetBlockCount()
	if err != nil {
		return &pb.BlockHeight{}, err
	}
	return &pb.BlockHeight{
		Height: h,
	}, nil
}

// EventGetAllMempool returns all mempool information
func (s *Server) EventGetAllMempool(_ *pb.Empty, stream pb.NodeCommuunications_EventGetAllMempoolServer) error {
	mp, err := s.BTCCli.GetAllMempool()
	if err != nil {
		return err
	}

	for _, rec := range mp {
		stream.Send(&pb.MempoolRecord{
			Category: int32(rec.Category),
			HashTX:   rec.HashTx,
		})
	}
	return nil
}

// EventResyncAddress is a method for resyncing address
func (s *Server) EventResyncAddress(c context.Context, address *pb.AddressToResync) (*pb.ReplyInfo, error) {
	log.Debugf("EventResyncAddress")
	allResync := []store.ResyncTx{}
	requestTimes := 0
	addrInfo, err := s.BTCApi.GetAddrFull(address.Address, map[string]string{"limit": "50"})
	if err != nil {
		return nil, fmt.Errorf("EventResyncAddress: s.BTCApi.GetAddrFull : %s", err.Error())
	}

	log.Debugf("EventResyncAddress:s.BTCApi.GetAddrFull")
	if addrInfo.FinalNumTX > 50 {
		requestTimes = int(float64(addrInfo.FinalNumTX) / 50.0)
	}

	for _, tx := range addrInfo.TXs {
		allResync = append(allResync, store.ResyncTx{
			Hash:        tx.Hash,
			BlockHeight: tx.BlockHeight,
		})
	}
	for i := 0; i < requestTimes; i++ {
		addrInfo, err := s.BTCApi.GetAddrFull(address.Address, map[string]string{"limit": "50", "before": strconv.Itoa(allResync[len(allResync)-1].BlockHeight)})
		if err != nil {
			return nil, fmt.Errorf("[ERR] EventResyncAddress: s.BTCApi.GetAddrFull : %s", err.Error())
		}
		for _, tx := range addrInfo.TXs {
			allResync = append(allResync, store.ResyncTx{
				Hash:        tx.Hash,
				BlockHeight: tx.BlockHeight,
			})
		}
	}

	reverseResyncTx(allResync)
	log.Debugf("EventResyncAddress:reverseResyncTx %d", len(allResync))

	s.BTCCli.ResyncAddresses(allResync, address)

	// for _, reTx := range allResync {
	// 	txHash, err := chainhash.NewHashFromStr(reTx.Hash)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("resyncAddress: chainhash.NewHashFromStr = %s", err.Error())
	// 	}

	// 	rawTx, err := s.BTCCli.RPCClient.GetRawTransactionVerbose(txHash)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("resyncAddress: RPCClient.GetRawTransactionVerbose = %s", err.Error())
	// 	}
	// 	s.BTCCli.ProcessTransaction(int64(reTx.BlockHeight), rawTx, true)
	// 	log.Debugf("EventResyncAddress:ProcessTransaction %d", len(allResync))
	// }

	return &pb.ReplyInfo{
		Message: "ok",
	}, nil
}

// EventSendRawTx sends raw TX
func (s *Server) EventSendRawTx(c context.Context, tx *pb.RawTx) (*pb.ReplyInfo, error) {
	hash, err := s.BTCCli.RPCClient.SendCyberRawTransaction(tx.Transaction, true)
	if err != nil {
		return &pb.ReplyInfo{
			Message: "err: wrong raw tx",
		}, fmt.Errorf("err: wrong raw tx %s", err.Error())

	}

	return &pb.ReplyInfo{
		Message: hash.String(),
	}, nil

}

// EventDeleteMempool removes mempool information
func (s *Server) EventDeleteMempool(_ *pb.Empty, stream pb.NodeCommuunications_EventDeleteMempoolServer) error {
	for del := range s.BTCCli.DeleteMempool {
		err := stream.Send(&del)
		if err != nil {
			//HACK:
			log.Errorf("Delete mempool record %s", err.Error())
			i := 0
			for {
				err := stream.Send(&del)
				if err != nil {
					i++
					log.Errorf("Delete mempool record resend attempt(%d) err = %s", i, err.Error())
					time.Sleep(time.Second * 2)
					if i == 3 {
						break
					}
				} else {
					log.Debugf("Delete mempool record resend success on %d attempt", i)
					break
				}
			}
		}
	}
	return nil
}

// EventAddMempoolRecord adds mempool record
func (s *Server) EventAddMempoolRecord(_ *pb.Empty, stream pb.NodeCommuunications_EventAddMempoolRecordServer) error {
	for add := range s.BTCCli.AddToMempool {
		err := stream.Send(&add)
		if err != nil {
			// HACK:
			log.Errorf("Add mempool record %s", err.Error())
			i := 0
			for {
				err := stream.Send(&add)
				if err != nil {
					i++
					log.Errorf("Add mempool record resend attempt(%d) err = %s", i, err.Error())
					time.Sleep(time.Second * 2)
					if i == 3 {
						break
					}
				} else {
					log.Debugf("Add mempool record resend success on %d attempt", i)
					break
				}
			}
		}
	}
	return nil
}

// EventDeleteSpendableOut removes SpOuts
func (s *Server) EventDeleteSpendableOut(_ *pb.Empty, stream pb.NodeCommuunications_EventDeleteSpendableOutServer) error {
	for delSp := range s.BTCCli.DelSpOut {
		log.Infof("Delete spendable out %v", delSp.String())
		err := stream.Send(&delSp)
		if err != nil {
			//HACK:
			log.Errorf("Delete spendable out %s", err.Error())
			i := 0
			for {
				err := stream.Send(&delSp)
				if err != nil {
					i++
					log.Errorf("Delete spendable out resend attempt(%d) err = %s", i, err.Error())
					time.Sleep(time.Second * 2)
					if i == 3 {
						break
					}
				} else {
					log.Debugf("EventDeleteSpendableOut history resend success on %d attempt", i)
					break
				}
			}
		}

	}
	return nil
}

// EventAddSpendableOut adds SpOuts
func (s *Server) EventAddSpendableOut(_ *pb.Empty, stream pb.NodeCommuunications_EventAddSpendableOutServer) error {

	for addSp := range s.BTCCli.AddSpOut {
		log.Infof("Add spendable out %v", addSp.String())
		err := stream.Send(&addSp)
		if err != nil {
			// HACK:
			log.Errorf("Add spendable out %s", err.Error())
			i := 0
			for {
				err := stream.Send(&addSp)
				if err != nil {
					i++
					log.Errorf("Add spendable out resend attempt(%d) err = %s", i, err.Error())
					time.Sleep(time.Second * 2)
				} else {
					log.Debugf("Add spendable out resend success on %d attempt", i)
					break
				}
			}

		}

	}

	return nil
}

// NewTx provides new TXs
func (s *Server) NewTx(_ *pb.Empty, stream pb.NodeCommuunications_NewTxServer) error {

	for tx := range s.BTCCli.TransactionsCh {
		log.Infof("NewTx history - %v", tx.String())
		err := stream.Send(&tx)
		if err != nil {
			// HACK:
			log.Errorf("NewTx history %s", err.Error())
			i := 0
			for {
				err := stream.Send(&tx)
				if err != nil {
					i++
					log.Errorf("NewTx history resend attempt(%d) err = %s", i, err.Error())
					time.Sleep(time.Second * 2)
				} else {
					log.Debugf("NewTx history resend success on %d attempt", i)
					break
				}
			}

		}
	}
	return nil
}

// ResyncAddress resyncs selected address
func (s *Server) ResyncAddress(_ *pb.Empty, stream pb.NodeCommuunications_ResyncAddressServer) error {
	for res := range s.BTCCli.ResyncCh {
		log.Infof("Resync address - %v", res.String())
		err := stream.Send(&res)
		if err != nil {
			// HACK:
			log.Errorf("Resync address %s", err.Error())
			i := 0
			for {
				err := stream.Send(&res)
				if err != nil {
					i++
					log.Errorf("Resync address resend attempt(%d) err = %s", i, err.Error())
					time.Sleep(time.Second * 2)
				} else {
					log.Debugf("Resync address resend success on %d attempt", i)
					break
				}
			}

		}
	}
	return nil
}
