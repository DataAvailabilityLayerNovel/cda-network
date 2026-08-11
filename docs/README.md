# CDA Network Documentation Hub

Welcome to the technical documentation and testing portal for **CDA Network**.

This documentation is divided into two main categories: **System Documentation** and **Testing & Measurement Documentation**.

---

## 1. Tài Liệu Hệ Thống (System Documentation)

Các tài liệu mô tả kiến trúc, giao thức truyền thông, thuật toán toán học và lộ trình phát triển:

- **[Kiến Trúc Tổng Thể (System Architecture)](file:///home/ubuntu/cda-network/docs/system/arch.md)**: 
  Mô hình phân tầng hệ thống (Layer L2, Routing Bootstrap, Storage Custody, Light Clients), cấu trúc ma trận ODS/EDS và quy trình luân chuyển dữ liệu.
- **[Giao Thức Mạng P2P & Định Tuyến (Networking & P2P Topology)](file:///home/ubuntu/cda-network/docs/system/network.md)**: 
  Cơ chế GossipSub Subnets theo cột, thuật toán Verifiable Custody Address Mapping và giao thức đồng bộ Active Pull.
- **[Mô Tả & Đặc Tả Kỹ Thuật Hệ Thống Thực Tế (Detailed System & Implementation Spec)](file:///home/ubuntu/cda-network/docs/system/technical_spec.md)**: 
  Tài liệu mô tả chi tiết kiến trúc hiện thực thực tế của toàn bộ codebase: vị trí file mã nguồn, pipeline xử lý dữ liệu, giao thức stream P2P, cơ chế quản lý custody/pruning, các tính năng đã hoàn thiện và hạn chế kỹ thuật của từng loại node (Publisher, Bootstrap, Store, Light).
- **[Đánh Giá Hệ Thống & Lộ Trình Triển Khai Thực Tế (Evaluation & Production Roadmap)](file:///home/ubuntu/cda-network/docs/system/evaluation_and_production_roadmap.md)**: 
  Phân tích chuyên sâu về ưu điểm, điểm nghẽn kỹ thuật và lộ trình 4 giai đoạn (Phase 1 → Phase 4) để đưa CDA Network lên môi trường Production quy mô lớn.
- **[Kế Hoạch & Lộ Trình Phát Triển (Architecture & Engineering Plan)](file:///home/ubuntu/cda-network/docs/system/plan.md)**: 
  Mục tiêu thiết kế, phân rã các module mã nguồn và kế hoạch tích hợp sản phẩm.

---

## 2. Tài Liệu Kiểm Thử & Đo Lường (Testing & Measurement Documentation)

Các kịch bản kiểm thử tích hợp (E2E), khả năng phục hồi lỗi, bảo mật và hướng dẫn đo lường hiệu năng:

- **[Kế Hoạch & Hướng Dẫn Thực Thi Kiểm Thử Toàn Diện (Deployment & Test Master Plan)](file:///home/ubuntu/cda-network/docs/testing/deployment_and_test_plan.md)**: 
  Đặc tả các kịch bản kiểm thử cốt lõi (Single-Seed Discovery, Join/Leave Lifecycle, Docker Compose Matrix, Byzantine Defending, Distributed Reconstruction) và hệ thống chỉ số Prometheus & Grafana.
- **[Hướng Dẫn Kịch Bản 3: Phục Hồi Dữ Liệu Khối Phân Tán (Distributed Reconstruction Test Guide)](file:///home/ubuntu/cda-network/docs/testing/scenario_3_reconstruction_test_guide.md)**: 
  Hướng dẫn chi tiết chạy kịch bản kiểm thử mất toàn bộ cột mạng và tự động tái tạo 100% dữ liệu gốc ODS bằng giải mã ngang Reed-Solomon.
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

# 5. Tiện ích xuất bản và lấy mẫu thủ công:
# ./scripts/publish.sh manual-block-1 http://localhost:8080
# ./scripts/das.sh manual-block-1 http://localhost:8499
```

