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

---

## 2. Tài Liệu Kiểm Thử & Đo Lường (Testing & Measurement Documentation)

Các kịch bản kiểm thử tích hợp (E2E), khả năng phục hồi lỗi, bảo mật và hướng dẫn đo lường hiệu năng:

- **[Hướng Dẫn Kiểm Thử Toàn Mạng E2E Đa Cột & Đa Tham Số (Full Network E2E Test Guide)](file:///home/ubuntu/cda-network/docs/testing/full_network_e2e_guide.md)**:
  Hướng dẫn toàn diện chạy kịch bản kiểm thử tích hợp 4 tầng (CometBFT -> Publisher -> Store Nodes -> Light Node Auto-DAS) với tùy biến $K$, $K_{\text{piece}}$, $ActiveCols$, $Blocks$.
- **[Hướng Dẫn Kiểm Thử Ghi Nhận Hoàn Thành & DAS (Completion & DAS Test Guide)](file:///home/ubuntu/cda-network/docs/testing/completion_test_guide.md)**: 
  Quy trình kiểm thử trạng thái `IsComplete` lưu trữ custody tại Store Node và ghi nhận log lấy mẫu thành công tại Light Node.

---

## 3. 🚀 Khởi Chạy Nhanh Các Kịch Bản Kiểm Thử (Quick Start Test Suites)

```bash
# 1. Kiểm thử Khám phá Ma trận Đơn Seed cho Light Node (32 Cột, 1024 Ô EDS)
bash scripts/tests/test_light_single_seed_das.sh

# 2. Kiểm thử Vòng đời Tham gia, Rời mạng & Sập nguồn đột ngột Store Node
bash scripts/tests/test_store_join_leave_lifecycle.sh

# 3. Kiểm thử Tích hợp Cụm Docker Compose E2E
bash scripts/tests/run_docker_test.sh

# 4. Kiểm thử Phục Hồi Dữ Liệu Khối Mất Toàn Bộ Cột Mạng (Kịch bản 3)
bash scripts/tests/test_scenario_3.sh

# 5. Kiểm thử E2E Toàn Mạng (CometBFT Consensus -> Publisher -> Store Custody -> Light Node Auto-DAS):
# Chạy mặc định (K=8, K_piece=4, Blocks=3, Txs=16):
./scripts/tests/test_full_network_e2e.sh
# Hoặc truyền cờ tùy chỉnh:
./scripts/tests/test_full_network_e2e.sh -k 16 -b 5
./scripts/tests/test_full_network_e2e.sh --help

# 6. Tiện ích xuất bản và lấy mẫu thủ công:
# ./scripts/publish.sh manual-block-1 http://localhost:8080
# ./scripts/das.sh manual-block-1 http://localhost:8499
```

