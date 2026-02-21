/*
Copyright 2024 The HAMi Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/spf13/cobra"
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/scheduler"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/routes"
	"github.com/Project-HAMi/HAMi/pkg/util"
	"github.com/Project-HAMi/HAMi/pkg/util/client"
	"github.com/Project-HAMi/HAMi/pkg/util/flag"
	"github.com/Project-HAMi/HAMi/pkg/util/nodelock"
	"github.com/Project-HAMi/HAMi/pkg/version"
)

//var version string

var (
	sher            *scheduler.Scheduler
	tlsKeyFile      string
	tlsCertFile     string
	enableProfiling bool
	rootCmd         = &cobra.Command{
		Use:   "scheduler",
		Short: "kubernetes vgpu scheduler",
		RunE: func(cmd *cobra.Command, args []string) error {
			flag.PrintPFlags(cmd.Flags())
			return start()
		},
	}
)

func init() {
	rootCmd.Flags().SortFlags = false
	rootCmd.PersistentFlags().SortFlags = false

	rootCmd.Flags().StringVar(&config.HTTPBind, "http_bind", "127.0.0.1:8080", "http server bind address")
	rootCmd.Flags().StringVar(&tlsCertFile, "cert_file", "", "tls cert file")
	rootCmd.Flags().StringVar(&tlsKeyFile, "key_file", "", "tls key file")
	rootCmd.Flags().StringVar(&config.SchedulerName, "scheduler-name", "", "the name to be added to pod.spec.schedulerName if not empty")
	rootCmd.Flags().Int32Var(&config.DefaultMem, "default-mem", 0, "default gpu device memory to allocate")
	rootCmd.Flags().Int32Var(&config.DefaultCores, "default-cores", 0, "default gpu core percentage to allocate")
	rootCmd.Flags().Int32Var(&config.DefaultResourceNum, "default-gpu", 1, "default gpu to allocate")
	rootCmd.Flags().StringVar(&config.NodeSchedulerPolicy, "node-scheduler-policy", util.NodeSchedulerPolicyBinpack.String(), "node scheduler policy")
	rootCmd.Flags().StringVar(&device.GPUSchedulerPolicy, "gpu-scheduler-policy", util.GPUSchedulerPolicySpread.String(), "GPU scheduler policy")
	rootCmd.Flags().StringVar(&config.MetricsBindAddress, "metrics-bind-address", ":9395", "The TCP address that the scheduler should bind to for serving prometheus metrics(e.g. 127.0.0.1:9395, :9395)")
	rootCmd.Flags().StringToStringVar(&config.NodeLabelSelector, "node-label-selector", nil, "key=value pairs separated by commas")

	rootCmd.Flags().Float32Var(&config.QPS, "kube-qps", client.DefaultQPS, "QPS to use while talking with kube-apiserver.")
	rootCmd.Flags().IntVar(&config.Burst, "kube-burst", client.DefaultBurst, "Burst to use while talking with kube-apiserver.")
	rootCmd.Flags().IntVar(&config.Timeout, "kube-timeout", client.DefaultTimeout, "Timeout to use while talking with kube-apiserver.")
	rootCmd.Flags().BoolVar(&enableProfiling, "profiling", false, "Enable pprof profiling via HTTP server")
	rootCmd.Flags().DurationVar(&config.NodeLockTimeout, "node-lock-timeout", time.Minute*5, "timeout for node locks")
	rootCmd.Flags().BoolVar(&config.ForceOverwriteDefaultScheduler, "force-overwrite-default-scheduler", true, "Overwrite schedulerName in Pod Spec when set to the const DefaultSchedulerName in https://k8s.io/api/core/v1 package")

	rootCmd.Flags().BoolVar(&config.LeaderElect, "leader-elect", false, "The pod of hami-scheduler enable leader select")
	rootCmd.Flags().StringVar(&config.LeaderElectResourceName, "leader-elect-resource-name", "", "The name of resource object that is used for leader election")
	rootCmd.Flags().StringVar(&config.LeaderElectResourceNamespace, "leader-elect-resource-namespace", "", "The namespace of resource object that is used for leader election")

	rootCmd.PersistentFlags().AddGoFlagSet(config.GlobalFlagSet())
	rootCmd.AddCommand(version.VersionCmd)
	rootCmd.Flags().AddGoFlagSet(util.InitKlogFlags())
}

// injectProfilingRoute 注入性能分析路由
// 用于运行时性能诊断
//
// 功能说明：
// - 注册 pprof 的各种分析接口
// - 支持 CPU、内存、goroutine 等分析
//
// 可用的分析接口：
// - /debug/pprof/: 概览页面
// - /debug/pprof/cmdline: 命令行参数
// - /debug/pprof/profile: CPU 性能分析（30秒采样）
// - /debug/pprof/symbol: 符号表查询
// - /debug/pprof/trace: 执行追踪
// - /debug/pprof/heap: 堆内存分析
// - /debug/pprof/goroutine: goroutine 堆栈
//
// 安全注意事项：
// - 生产环境谨慎启用
// - 可能暴露敏感信息
// - 建议只在内网访问
//
// 参数：
// - router: HTTP 路由器
func injectProfilingRoute(router *httprouter.Router) {
	router.GET("/debug/pprof/*suffix", func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		suffix := params.ByName("suffix")
		switch suffix {
		case "/cmdline":
			pprof.Cmdline(w, r)
		case "/profile":
			pprof.Profile(w, r)
		case "/symbol":
			pprof.Symbol(w, r)
		case "/trace":
			pprof.Trace(w, r)
		default:
			pprof.Index(w, r)
		}
	})
}

// +scheduler:entry
// start 启动调度器服务
// 这是整个调度系统的启动入口
//
// 启动流程：
// 1. 初始化配置：节点锁超时、Kubernetes 客户端
// 2. 初始化设备：加载设备配置，注册设备类型
// 3. 创建调度器：NewScheduler() 创建核心调度器实例
// 4. 启动后台任务：
//   - RegisterFromNodeAnnotations: 持续监听节点变化
//   - Start: 启动 Informer 和事件处理器
//
// 5. 启动监控：Prometheus 指标服务
// 6. 启动 HTTP 服务：注册路由，监听请求
//
// 为什么要先初始化设备？
// - 调度器需要知道支持哪些设备类型
// - 设备配置决定了资源名称和调度策略
// - 提前加载避免运行时错误
//
// 为什么 RegisterFromNodeAnnotations 要在 goroutine 中？
// - 这是一个无限循环的后台任务
// - 不能阻塞主流程
// - 持续监听节点变化并更新状态
//
// 为什么要 defer sher.Stop()？
// - 确保优雅退出时清理资源
// - 关闭 Informer 和 goroutine
// - 避免资源泄漏
//
// HTTP vs HTTPS：
// - 如果提供了证书，启动 HTTPS 服务
// - 使用 certwatcher 支持证书热重载
// - 生产环境建议使用 HTTPS
//
// Profiling 的作用：
// - 启用后可以访问 /debug/pprof/ 查看性能分析
// - 用于排查性能问题和内存泄漏
// - 生产环境谨慎启用（有安全风险）
//
// 返回值：
// - error: 启动过程中的错误
func start() error {
	// Initialize node lock timeout from config
	nodelock.NodeLockTimeout = config.NodeLockTimeout
	klog.InfoS("Set node lock timeout", "timeout", nodelock.NodeLockTimeout)
	client.InitGlobalClient(
		client.WithBurst(config.Burst),
		client.WithQPS(config.QPS),
		client.WithTimeout(config.Timeout),
	)

	config.InitDevices()

	var err error
	config.HostName, err = os.Hostname()
	if err != nil {
		return fmt.Errorf("unable to get hostname: %v", err)
	}
	if config.HostName == "" {
		return fmt.Errorf("empty hostname returned")
	}

	sher = scheduler.NewScheduler()
	go sher.RegisterFromNodeAnnotations()
	err = sher.Start()
	if err != nil {
		return err
	}
	defer sher.Stop()

	// start monitor metrics
	go initMetrics(config.MetricsBindAddress)

	// start http server
	router := httprouter.New()
	router.POST("/filter", routes.PredicateRoute(sher))
	router.POST("/bind", routes.Bind(sher))
	router.POST("/webhook", routes.WebHookRoute())
	router.GET("/healthz", routes.HealthzRoute())
	router.GET("/readyz", routes.ReadyzRoute(sher))
	klog.Info("listen on ", config.HTTPBind)

	if enableProfiling {
		injectProfilingRoute(router)
		klog.Infof("Profiling enabled, visit %s/debug/pprof/ to view profiles", config.HTTPBind)
	}

	if len(tlsCertFile) == 0 || len(tlsKeyFile) == 0 {
		if err := http.ListenAndServe(config.HTTPBind, router); err != nil {
			return fmt.Errorf("listen and Serve error, %v", err)
		}
	} else {
		certWatcher, err := certwatcher.New(tlsCertFile, tlsKeyFile)
		if err != nil {
			return fmt.Errorf("failed to create cert watcher: %w", err)
		}

		tlsCfg := &tls.Config{
			GetCertificate: certWatcher.GetCertificate,
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			if err := certWatcher.Start(ctx); err != nil && err != context.Canceled {
				klog.ErrorS(err, "cert watcher error")
			}
		}()

		addr := config.HTTPBind
		handler := router
		server := &http.Server{
			Addr:      addr,
			Handler:   handler,
			TLSConfig: tlsCfg,
		}
		klog.InfoS("Starting HTTPS server", "address", addr)
		if err := server.ListenAndServeTLS("", ""); err != nil {
			return fmt.Errorf("HTTPS server error: %w", err)
		}
	}
	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		klog.Fatal(err)
	}
}
