# CDA Network Documentation Hub

## 1. Tài Liệu Hệ Thống (System Documentation - [docs/system](file:///home/ubuntu/cda-network/docs/system/README.md))

Tài liệu mô tả kiến trúc, giao thức truyền thông, thuật toán toán học, đặc tả kỹ thuật và các giao diện CLI/curl API, được chia thành 3 đầu mục:

### 1.1. 🏗️ Kiến Trúc Hệ Thống (System Architecture)
- **[Kiến Trúc Tổng Thể (System Architecture)](file:///home/ubuntu/cda-network/docs/system/system_architecture/arch.md)**: 
  Mô hình phân tầng hệ thống (Layer L2, Routing Bootstrap, Storage Custody, Light Clients), cấu trúc ma trận ODS/EDS và quy trình luân chuyển dữ liệu.
- **[Kế Hoạch & Lộ Trình Phát Triển (Architecture & Engineering Plan)](file:///home/ubuntu/cda-network/docs/system/system_architecture/plan.md)**: 
  Mục tiêu thiết kế, phân rã các module mã nguồn và kế hoạch tích hợp sản phẩm.
- **[Tài Liệu Chuyển Đổi Kênh P2P (Per-Node Channels Migration)](file:///home/ubuntu/cda-network/docs/system/system_architecture/per_node_channels_migration.md)**: 
  Tài liệu mô tả kiến trúc chuyển đổi nâng cấp kênh giao tiếp P2P stream tách biệt theo từng node.
- **[Tài Liệu Tích Hợp Đồng Thuận CometBFT (CometBFT Integration & Architecture Spec)](file:///home/ubuntu/cda-network/docs/system/system_architecture/cometbft_cda_integration.md)**: 
  Tài liệu mô tả chi tiết hiện trạng tùy biến khối BFT trong CometBFT, sơ đồ luồng dữ liệu E2E và kế hoạch tích hợp thay thế bộ tạo khối giả lập.

### 1.2. 🌐 Kiến Trúc Mạng (Network Architecture)
- **[Giao Thức Mạng P2P & Định Tuyến Topology (Networking & P2P Topology)](file:///home/ubuntu/cda-network/docs/system/network_architecture/network.md)**: 
  Cơ chế GossipSub Subnets theo cột, thuật toán Verifiable Custody Address Mapping và giao thức đồng bộ Active Pull.

### 1.3. 🛠️ Tài Liệu Kỹ Thuật (Technical Documentation)
- **[Đặc Tả Kỹ Thuật Hệ Thống Thực Tế (Detailed System & Implementation Spec)](file:///home/ubuntu/cda-network/docs/system/technical_docs/technical_spec.md)**: 
  Tài liệu mô tả chi tiết kiến trúc hiện thực thực tế của toàn bộ codebase: vị trí file mã nguồn, pipeline xử lý dữ liệu, giao thức stream P2P, cơ chế quản lý custody/pruning.
- **[Hướng Dẫn CLI & Lệnh Curl REST API (CLI & Curl API Guide)](file:///home/ubuntu/cda-network/docs/system/technical_docs/cli_and_curl_guide.md)**: 
  Tài liệu mô tả chi tiết các lệnh `curl` HTTP REST API (Publisher, Light Node DAS, Bootstrap SSE) và các công cụ dòng lệnh CLI (`publish.sh`, `das.sh`, `bot_das.sh`, `block_publisher_bot.py`).
- **[Đặc Tả Thiết Lập Concurrency & Tối Ưu Phần Cứng (concurrency_and_hardware_tuning.md)](file:///home/ubuntu/cda-network/docs/system/technical_docs/concurrency_and_hardware_tuning.md)**: 
  Tài liệu tổng hợp các thiết lập concurrency (goroutines, semaphores, worker pools, batch size) và hướng dẫn tinh chỉnh cấu hình theo quy mô ma trận $K$.
- **[Đặc Tả Chi Tiết Các Node & Lan Truyền Ma Trận Mạng Store Node (node_architecture_and_matrix_propagation.md)](file:///home/ubuntu/cda-network/docs/system/technical_docs/node_architecture_and_matrix_propagation.md)**: 
  Tài liệu mô tả chi tiết nhiệm vụ tính toán, cấu hình song song / đồng thời, thiết lập mạng P2P/PubSub và phân tích chuyên sâu điểm nghẽn lan truyền dữ liệu trong ma trận Store Node.


---

## 2. Tài Liệu Kiểm Thử & Đo Lường (Testing & Measurement Documentation)

Các kịch bản kiểm thử tích hợp (E2E), khả năng phục hồi lỗi, bảo mật và hướng dẫn đo lường hiệu năng:

- **[Hướng Dẫn Cấu Hình Trình Sinh Docker Compose (`generate_compose.py`)](file:///home/ubuntu/cda-network/docs/testing/generate_compose_guide.md)**:
  Hướng dẫn chi tiết toàn bộ bảng cờ CLI, tối ưu hóa concurrency, quota CPU và tự động sinh topology mạng container hóa.
- **[Hướng Dẫn Vận Hành Kiểm Thử Docker E2E & Grafana](file:///home/ubuntu/cda-network/docs/testing/e2e/docker_e2e_guide.md)**:
  Kiểm thử tích hợp khép kín CometBFT -> CDA -> Auto-DAS trên container Docker và giám sát trực quan thời gian thực.
- **[Hướng Dẫn Kiểm Thử Toàn Mạng Native E2E](file:///home/ubuntu/cda-network/docs/testing/e2e/full_network_e2e_guide.md)**:
  Kiểm thử toàn mạng với tiến trình nền trực tiếp trên Host với tùy biến $K$, $K_{\text{piece}}$, $ActiveCols$, $Blocks$.
- **[Hướng Dẫn Đo Lường Hiệu Năng & Độ Trễ Xử Lý Khối (Benchmark Guide)](file:///home/ubuntu/cda-network/docs/testing/benchmark/benchmark_and_latency_guide.md)**:
  Đo đạc độ trễ xử lý khối (Pipeline 1-block-ahead, tuần tự, ma trận tự động) cho khối 2MB ($K=64$).
- **[Hướng Dẫn Kịch Bản Chịu Lỗi & Vòng Đời Node (Resilience Guide)](file:///home/ubuntu/cda-network/docs/testing/resilience/advanced_fault_tolerance_and_lifecycle_guide.md)**:
  Kiểm thử phục hồi dữ liệu khi mất 50% ma trận (Kịch bản 3), vòng đời Store Node (Join/Leave/Crash), và Single-seed discovery.

---

## 3. 🚀 Khởi Chạy Nhanh Các Kịch Bản Kiểm Thử (Quick Start Test Suites)

Xem mục lục hướng dẫn đầy đủ tại: **[Trung Tâm Tài Liệu Kiểm Thử (Testing Hub)](file:///home/ubuntu/cda-network/docs/testing/README.md)**

```bash
# 1. Kiểm thử E2E Container hóa với Docker & Grafana (CometBFT -> CDA -> Auto-DAS)
./scripts/tests/e2e/test_docker_e2e.sh -k 8 -p 4 -c 1 -s 4 -l 1 -b 3

# 2. Kiểm thử E2E Toàn Mạng Native (tiến trình nền trên Host)
./scripts/tests/e2e/test_full_network_e2e.sh -k 8 -p 4 -b 3

# 3. Đo lường hiệu năng xử lý khối (Benchmark Timing)
python3 scripts/tests/benchmark/test_pipeline_block_timing.py --k 64 --count 3 --cell-size 512

# 4. Kiểm thử phục hồi dữ liệu phân tán (Mất cột mạng & 50% ma trận - Kịch bản 3)
bash scripts/tests/resilience/test_scenario_3.sh

# 5. Kiểm thử vòng đời Store Node (Dynamic Join, Graceful Leave & Crash Cleanup)
bash scripts/tests/resilience/test_store_join_leave_lifecycle.sh

# 6. Kiểm thử luồng giao dịch đồng thuận CometBFT & CDA Header
bash scripts/tests/resilience/test_consensus_tx_flow.sh

# 7. Kiểm thử khám phá ma trận đơn seed cho Light Node (32 Cột, 1024 Ô EDS)
bash scripts/tests/resilience/test_light_single_seed_das.sh

# 8. Tiện ích xuất bản và lấy mẫu thủ công:
./scripts/publish.sh manual-block-1 http://localhost:8080 8 64
./scripts/das.sh manual-block-1 http://localhost:9401
```


