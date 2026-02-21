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

package scheduler

import (
	"context"
	"encoding/json"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/device/nvidia"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
)

const template = "Processing admission hook for pod %v/%v, UID: %v"

type webhook struct {
	decoder admission.Decoder
}

//+scheduler:1
// NewWebHook 创建一个新的 Webhook 处理器
// 这是调度流程的第一步：Pod 准入控制
// 
// 功能说明：
// 1. 初始化 Kubernetes 对象解码器，用于解析 AdmissionReview 请求
// 2. 创建 Webhook Handler，处理 Pod 创建/更新请求
//
// 为什么需要 Webhook？
// - 在 Pod 创建时就进行资源验证，避免无效的调度尝试
// - 自动为需要设备的 Pod 设置正确的 schedulerName
// - 提前检查资源配额，防止超分
//
// 返回值：
// - *admission.Webhook: 可以注册到 HTTP 路由的 Webhook 处理器
// - error: 初始化失败时返回错误
func NewWebHook() (*admission.Webhook, error) {
	logf.SetLogger(klog.NewKlogr())
	schema := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(schema); err != nil {
		return nil, err
	}
	decoder := admission.NewDecoder(schema)
	wh := &admission.Webhook{Handler: &webhook{decoder: decoder}}
	return wh, nil
}

//+scheduler:1.1
// Handle 处理 Pod 的准入请求（Webhook 的核心逻辑）
// 这是调度流程的第一步：在 Pod 创建时进行验证和修改
//
// 处理流程：
// 1. 解码 AdmissionReview 请求，获取 Pod 对象
// 2. 基本验证：检查容器列表、调度器名称、特权容器
// 3. 资源检测：遍历所有容器，检查是否请求了 AI 设备资源
// 4. 调度器设置：如果需要设备调度，设置 schedulerName 为 HAMi
// 5. 配额验证：检查命名空间的资源配额是否足够
// 6. 返回修改：将修改后的 Pod 以 JSON Patch 形式返回
//
// 为什么要检查 schedulerName？
// - 避免覆盖用户明确指定的其他调度器
// - 只处理需要设备调度的 Pod
// - 支持多调度器共存
//
// 为什么要检查特权容器？
// - 特权容器可以直接访问所有设备，不需要调度器分配
// - 避免与 Device Plugin 的设备隔离机制冲突
//
// 参数：
// - ctx: 请求上下文（未使用，保留用于超时控制）
// - req: Kubernetes AdmissionReview 请求
//
// 返回值：
// - admission.Response: 包含是否允许、错误信息、JSON Patch 的响应
func (h *webhook) Handle(_ context.Context, req admission.Request) admission.Response {
	pod := &corev1.Pod{}
	err := h.decoder.Decode(req, pod)
	if err != nil {
		klog.Errorf("Failed to decode request: %v", err)
		return admission.Errored(http.StatusBadRequest, err)
	}
	if len(pod.Spec.Containers) == 0 {
		klog.Warningf(template+" - Denying admission as pod has no containers", pod.Namespace, pod.Name, pod.UID)
		return admission.Denied("pod has no containers")
	}
	if pod.Spec.SchedulerName != "" &&
		(pod.Spec.SchedulerName != corev1.DefaultSchedulerName || !config.ForceOverwriteDefaultScheduler) &&
		(len(config.SchedulerName) == 0 || pod.Spec.SchedulerName != config.SchedulerName) {
		klog.Infof(template+" - Pod already has different scheduler assigned", req.Namespace, req.Name, req.UID)
		return admission.Allowed("pod already has different scheduler assigned")
	}
	klog.Infof(template, pod.Namespace, pod.Name, pod.UID)
	hasResource := false
	for idx, ctr := range pod.Spec.Containers {
		c := &pod.Spec.Containers[idx]
		if ctr.SecurityContext != nil {
			if ctr.SecurityContext.Privileged != nil && *ctr.SecurityContext.Privileged {
				klog.Warningf(template+" - Denying admission as container %s is privileged", pod.Namespace, pod.Name, pod.UID, c.Name)
				continue
			}
		}
		for _, val := range device.GetDevices() {
			found, err := val.MutateAdmission(c, pod)
			if err != nil {
				klog.Errorf("validating pod failed:%s", err.Error())
				return admission.Errored(http.StatusInternalServerError, err)
			}
			hasResource = hasResource || found
		}
	}

	if !hasResource {
		klog.Infof(template+" - Allowing admission for pod: no resource found", pod.Namespace, pod.Name, pod.UID)
		//return admission.Allowed("no resource found")
	} else if len(config.SchedulerName) > 0 {
		pod.Spec.SchedulerName = config.SchedulerName
		if pod.Spec.NodeName != "" {
			klog.Infof(template+" - Pod already has node assigned", pod.Namespace, pod.Name, pod.UID)
			return admission.Denied("pod has node assigned")
		}
	}
	if !fitResourceQuota(pod) {
		return admission.Denied("exceeding resource quota")
	}
	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		klog.Errorf(template+" - Failed to marshal pod, error: %v", pod.Namespace, pod.Name, pod.UID, err)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

//+scheduler:1.2
// fitResourceQuota 检查 Pod 是否符合命名空间的资源配额限制
// 这是准入控制的最后一步验证
//
// 功能说明：
// 1. 遍历 Pod 的所有容器，累加设备资源请求
// 2. 考虑内存放大因子（MemoryFactor），计算实际内存需求
// 3. 调用 QuotaManager 检查配额是否足够
//
// 为什么需要内存放大因子？
// - 某些设备（如 NVIDIA GPU）需要额外的系统内存用于驱动和缓冲
// - 放大因子确保配额检查考虑了这部分隐藏开销
// - 防止实际使用超过配额限制
//
// 为什么只支持 NVIDIA？
// - 目前只有 NVIDIA 设备配置了内存放大因子
// - 其他设备可以通过类似方式扩展
//
// 参数：
// - pod: 待检查的 Pod 对象
//
// 返回值：
// - bool: true 表示符合配额，false 表示超出配额
func fitResourceQuota(pod *corev1.Pod) bool {
	for deviceName, dev := range device.GetDevices() {
		// Only supports NVIDIA
		if deviceName != nvidia.NvidiaGPUDevice {
			continue
		}
		memoryFactor := nvidia.MemoryFactor
		resourceNames := dev.GetResourceNames()
		resourceName := corev1.ResourceName(resourceNames.ResourceCountName)
		memResourceName := corev1.ResourceName(resourceNames.ResourceMemoryName)
		coreResourceName := corev1.ResourceName(resourceNames.ResourceCoreName)
		var memoryReq int64 = 0
		var coresReq int64 = 0
		getRequest := func(ctr *corev1.Container, resName corev1.ResourceName) (int64, bool) {
			v, ok := ctr.Resources.Limits[resName]
			if !ok {
				v, ok = ctr.Resources.Requests[resName]
			}
			if ok {
				if n, ok := v.AsInt64(); ok {
					return n, true
				}
			}
			return 0, false
		}
		for _, ctr := range pod.Spec.Containers {
			req, ok := getRequest(&ctr, resourceName)
			if ok && req == 1 {
				if memReq, ok := getRequest(&ctr, memResourceName); ok {
					memoryReq += memReq
				}
				if coreReq, ok := getRequest(&ctr, coreResourceName); ok {
					coresReq += coreReq
				}
			}
		}
		if memoryFactor > 1 {
			oriMemReq := memoryReq
			memoryReq = memoryReq * int64(memoryFactor)
			klog.V(5).Infof("Adjusting memory request for quota check: oriMemReq %d, memoryReq %d, factor %d", oriMemReq, memoryReq, memoryFactor)
		}
		if !device.GetLocalCache().FitQuota(pod.Namespace, memoryReq, memoryFactor, coresReq, deviceName) {
			klog.Infof(template+" - Denying admission", pod.Namespace, pod.Name, pod.UID)
			return false
		}
	}
	return true
}
