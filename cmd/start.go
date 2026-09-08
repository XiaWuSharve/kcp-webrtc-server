/*
Copyright © 2025 XiaWuSharve <sharve@foxmail.com>
*/
package cmd

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/XiaWuSharve/whisperly/config"
	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/network/server"
	"github.com/bwmarrin/snowflake"
	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
	"github.com/xtaci/kcp-go/v5"
)

// startCmd represents the start command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a broadcast server. ",
	Long:  ``,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		config.Bind(cmd)
		node, err := snowflake.NewNode(config.Server.NodeId)
		if err != nil {
			panic(err)
		}
		datas.Ids = node
	},
	Run: func(cmd *cobra.Command, args []string) {
		cfg := config.Server
		protocol := cfg.Protocol
		host := cfg.Host
		port := cfg.Port
		var s server.Server
		var listener net.Listener
		var err error
		slog.Info("starting...", "protocol", protocol)
		switch protocol {
		case "kcp":
			listener, err = kcp.Listen(host + ":" + strconv.Itoa(port))
			if err != nil {
				slog.Error("cannot create listener", "err", err)
				// 处理错误...
			}
			s = server.NewKcpServer()
		case "websocket":
			&websocket.Upgrader{
				ReadBufferSize:  k.ReadBufferSize,
				WriteBufferSize: k.WriteBufferSize,
			}
			listener, err = net.Listen("tcp", host+":"+strconv.Itoa(port))
			if err != nil {
				slog.Error("cannot create listener", "err", err)
				// 处理错误...
			}
			s = server.NewWebSocketServer(cfg.ReadBufferSize, cfg.WriteBufferSize)
		default:
			slog.Error("protocol param cannot be", "value", protocol)
			return
		}

		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		signal.Notify(sig, syscall.SIGTERM)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-sig
			slog.Info("exiting...")
			cancel()
		}()
		if err := s.Start(ctx, listener); !errors.Is(err, net.ErrClosed) {
			slog.Error("cannot serve", "err", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(startCmd)

	// Here you will define your flags and configuration settings.
	config.Init(startCmd)
	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// startCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// startCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
