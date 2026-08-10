# Trung Tâm Tài Liệu Hệ Thống CDA Network (Documentation Hub)

Chào mừng bạn đến với trung tâm tài liệu kỹ thuật và hướng dẫn kiểm thử của dự án **CDA Network (Coded Data Availability Network)**.

Tài liệu được phân loại thành hai nhóm chuyên biệt: **Tài Liệu Hệ Thống (System Docs)** và **Tài Liệu Kiểm Thử & Đo Lường (Testing Docs)**.

---

## 1. 🏗️ Tài Liệu Hệ Thống (System Documentation)

Các tài liệu mô tả kiến trúc, giao thức truyền thông, thuật toán toán học và lộ trình phát triển:

- 📄 **[Kiến Trúc Tổng Thể (System Architecture)](file:///home/ubuntu/cda-network/docs/system/arch.md)**: 
  Mô hình phân tầng hệ thống (Layer L2, Routing Bootstrap, Storage Custody, Light Clients), cấu trúc ma trận ODS/EDS và quy trình luân chuyển dữ liệu.
- 📄 **[Giao Thức Mạng P2P & Định Tuyến (Networking & P2P Topology)](file:///home/ubuntu/cda-network/docs/system/network.md)**: 
  Cơ chế GossipSub Subnets theo cột, thuật toán Verifiable Custody Address Mapping và giao thức đồng bộ Active Pull.
- 📄 **[Đặc Tả Kỹ Thuật & Toán Học (Technical & Mathematical Spec)](file:///home/ubuntu/cda-network/docs/system/technical_spec.md)**: 
  Chi tiết về mã hóa 2D Reed-Solomon (Leopard Codec), Random Linear Network Coding (RLNC), cam kết KZG Opening Proofs và giải mã đại số.
- 📄 **[Kế Hoạch & Lộ Trình Phát Triển (Architecture & Engineering Plan)](file:///home/ubuntu/cda-network/docs/system/plan.md)**: 
  Mục tiêu thiết kế, phân rã các module mã nguồn và kế hoạch tích hợp sản phẩm.

---

## 2. 🧪 Tài Liệu Kiểm Thử & Đo Lường (Testing & Measurement Documentation)

Các kịch bản kiểm thử tích hợp (E2E), khả năng phục hồi lỗi, bảo mật và hướng dẫn đo lường hiệu năng:

- 📄 **[Kế Hoạch Kiểm Thử Toàn Diện (Deployment & Test Master Plan)](file:///home/ubuntu/cda-network/docs/testing/deployment_and_test_plan.md)**: 
  Đặc tả 3 kịch bản kiểm thử cốt lõi (Failover, Byzantine Defending, Distributed Reconstruction) và hệ thống chỉ số đo lường hiệu năng (Prometheus & Grafana).
- 📄 **[Hướng Dẫn Kịch Bản 3: Phục Hồi Dữ Liệu Khối Phân Tán (Distributed Reconstruction Test Guide)](file:///home/ubuntu/cda-network/docs/testing/scenario_3_reconstruction_test_guide.md)**: 
  Hướng dẫn chi tiết chạy kịch bản kiểm thử mất toàn bộ cột mạng và tự động tái tạo 100% dữ liệu gốc ODS bằng giải mã ngang Reed-Solomon.
- 📄 **[Hướng Dẫn Kiểm Thử Ghi Nhận Hoàn Thành & DAS (Completion & DAS Test Guide)](file:///home/ubuntu/cda-network/docs/testing/completion_test_guide.md)**: 
  Quy trình kiểm thử trạng thái `IsComplete` lưu trữ custody tại Store Node và ghi nhận log lấy mẫu thành công tại Light Node.

---

## 3. 🚀 Khởi Chạy Nhanh (Quick Start)

```bash
# Khởi động toàn bộ mạng lưới (8 cột, 8 store/cột, 1 light node)
python3 scripts/generate_compose.py --cols 8 --active-cols 1 --stores-per-col 8 --lights 1 --k 16 --k-piece 4 --prune-enable --prune-ttl 15s
docker compose -f docker-compose.json up -d --build

# Chạy kiểm thử Phục Hồi Dữ Liệu Khối (Kịch bản 3)
bash scripts/tests/test_scenario_3.sh

# Mở Dashboard Giám Sát Hiệu Năng Thời Gian Thực
# Truy cập: http://localhost:3000
```
