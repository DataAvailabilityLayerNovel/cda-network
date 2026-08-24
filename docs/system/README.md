# Cổng Tài Liệu Hệ Thống CDA Network (System Documentation)

Tài liệu trong thư mục này được phân rã thành 3 đầu mục chuyên sâu phục vụ nghiên cứu, phát triển và triển khai hệ thống **CDA Network**:

---

## 1. 🏗️ Kiến Trúc Hệ Thống (System Architecture)

Các tài liệu về mô hình phân tầng, cấu trúc ma trận dữ liệu ODS/EDS, kế hoạch kiến trúc và cơ chế phân chia kênh giao tiếp P2P:

- **[Kiến Trúc Tổng Thể Hệ Thống (arch.md)](file:///home/ubuntu/cda-network/docs/system/system_architecture/arch.md)**:
  Mô hình phân tầng hệ thống (Layer L2, Routing Bootstrap, Storage Custody, Light Clients), cấu trúc ma trận ODS/EDS và quy trình luân chuyển dữ liệu.
- **[Kế Hoạch & Lộ Trình Phát Triển (plan.md)](file:///home/ubuntu/cda-network/docs/system/system_architecture/plan.md)**:
  Mục tiêu thiết kế, phân rã các module mã nguồn và kế hoạch tích hợp sản phẩm.
- **[Tài Liệu Chuyển Đổi Kênh P2P Theo Node (per_node_channels_migration.md)](file:///home/ubuntu/cda-network/docs/system/system_architecture/per_node_channels_migration.md)**:
  Đặc tả kiến trúc chuyển đổi nâng cấp kênh giao tiếp P2P stream tách biệt theo từng node.

---

## 2. 🌐 Kiến Trúc Mạng (Network Architecture)

Tài liệu về giao thức truyền thông P2P, định tuyến LibP2P và cơ chế GossipSub Mesh:

- **[Giao Thức Mạng P2P & Định Tuyến Topology (network.md)](file:///home/ubuntu/cda-network/docs/system/network_architecture/network.md)**:
  Cơ chế GossipSub Subnets theo cột, thuật toán Verifiable Custody Address Mapping và giao thức đồng bộ Active Pull.

---

## 3. 🛠️ Tài Liệu Kỹ Thuật (Technical Documentation)

Đặc tả hiện thực kỹ thuật chi tiết của mã nguồn và hướng dẫn các giao diện CLI / REST API `curl`:

- **[Đặc Tả Kỹ Thuật Hệ Thống Thực Tế (technical_spec.md)](file:///home/ubuntu/cda-network/docs/system/technical_docs/technical_spec.md)**:
  Tài liệu mô tả chi tiết kiến trúc hiện thực thực tế của toàn bộ codebase: vị trí file mã nguồn, pipeline xử lý dữ liệu, giao thức stream P2P, cơ chế quản lý custody/pruning.
- **[Hướng Dẫn CLI & Lệnh Curl REST API (cli_and_curl_guide.md)](file:///home/ubuntu/cda-network/docs/system/technical_docs/cli_and_curl_guide.md)**:
  Tài liệu mô tả chi tiết các lệnh `curl` HTTP REST API (Publisher, Light, Bootstrap SSE) và bộ script tự động hóa CLI (`publish.sh`, `das.sh`, `bot_das.sh`, `block_publisher_bot.py`).
