# Báo Cáo Kỹ Thuật: Tối Ưu Hóa Thuật Toán FK20 (Feist-Khovratovich) Cho Sinh Bằng Chứng Mở KZG Amortized

> [!NOTE]
> **Trạng thái: ĐÃ XỬ LÝ (Resolved / Completed)**  
> **Ngày giải quyết:** 2026-09-30  
> **Phạm vi:** `rlnc-rsmt2d/cda` (`fk20.go`, `gnark.go`, `kzg.go`, `publisher.go`), `cda-bootstrap-node/internal/engine` (`proof_generator.go`, `rlnc_encoder.go`), `cda-store-node`, `cda-light-node`  
> **Mức độ ảnh hưởng:** **Nghiêm trọng (Critical)** — Giải quyết triệt để điểm nghẽn lớn nhất trong quy trình xử lý block của Bootstrap Node, giảm thời gian sinh KZG proof từ ~3.0s xuống dưới 150ms.

---

## 1. Tổng Quan Điểm Nghẽn (Bottleneck Statement)

Khi phân tích dữ liệu xử lý thực tế của cụm CDA Network tại Block 1 và Block 2 (cấu hình ma trận $K=64$, EDS $N=128$, $k_{\text{piece}}=8$):
- **Thời gian xử lý Block 2:** ~5.0 giây.
- **Điểm nghẽn chi phối (Dominated Bottleneck):** Bước sinh KZG Opening Proofs tại Bootstrap Node (`GenerateColumnProofs`) chiếm **3.013 giây** (hơn 60% tổng thời gian xử lý toàn block).
- **Nguyên nhân toán học:**
  - Bootstrap Node cần sinh proof cho $N = 128$ ô trên một cột dữ liệu.
  - Mỗi ô bao gồm $k_{\text{piece}} = 8$ mảnh.
  - Tổng số opening proof cần sinh cho 1 cột: $N \times k = 128 \times 8 = 1024$ proofs.
  - Thuật toán ngây thơ (Naive) gọi hàm `kzg.Open()` độc lập 1024 lần. Mỗi lần chia đa thức bậc 128 và nhân vô hướng MSM tốn xấp xỉ 2.5 - 3.0ms $\implies$ tổng cộng tốn hơn **3,000ms**.

---

## 2. Giải Pháp: Triển Khai Thuật Toán FK20 (Feist-Khovratovich)

Thuật toán FK20 tận dụng tính chất của các nghiệm đơn vị (roots of unity) trong trường hữu hạn $\mathbb{F}_r$ và biến đổi Fourier nhanh (FFT) trên đường cong Elliptic để tính đồng thời toàn bộ $N$ opening proofs cho một đa thức $P(X)$ với độ phức tạp $O(N \log N)$ thay vì $O(N^2)$.

### 2.1 Cơ sở toán học của FK20
Cho đa thức $P(X) = \sum_{i=0}^{N-1} a_i X^i$. Opening proof tại điểm $z = \omega^r$ là cam kết thương:
$$q_r(X) = \frac{P(X) - P(\omega^r)}{X - \omega^r}$$
Hệ số của đa thức thương được biểu diễn qua ma trận Toeplitz nhân với vector hệ số $a$. Nhúng ma trận Toeplitz vào ma trận Circulant kích thước $2N$ cho phép tính toán thông qua tích chập:
$$\mathbf{C} = \text{IFFT}_{2N}\left(\text{FFT}_{2N}(\mathbf{A}) \odot \text{FFT}_{2N}(\mathbf{B})\right)$$
trong đó:
- $\mathbf{B} = [[s^0]_1, [s^1]_1, \dots, [s^{N-2}]_1, 0, \dots, 0] \in G_1^{2N}$ (precomputed và cached trong `FK20Engine`).
- $\mathbf{A} = [a_{N-1}, a_{N-2}, \dots, a_1, 0, \dots, 0] \in \mathbb{F}_r^{2N}$.

Sau khi thu được vector $\mathbf{h} = [C_{N-2}, C_{N-3}, \dots, C_0, 0] \in G_1^N$, toàn bộ $N$ opening proofs được tính bằng một phép biến đổi Fourier thuận trên nhóm $G_1$:
$$\boldsymbol{\pi} = \text{FFT}_N(\mathbf{h})$$

---

## 3. Các Tối Ưu Kỹ Thuật Đã Áp Dụng (Applied Optimizations)

Trong quá trình cài đặt, micro-benchmark sơ bộ ban đầu cho thấy FFT $G_1$ ngây thơ bị chậm do chi phí nhân vô hướng. Chúng tôi đã tiến hành tối ưu hóa chuyên sâu:

1. **Bỏ qua nhân vô hướng khi $w=1$ ($j=0$ Bypass):**
   - Trong thuật toán DIF FFT trên $G_1$, tại tất cả các tầng, hệ số twiddle đầu tiên luôn là $w^0 = 1$.
   - Bỏ qua hoàn toàn phép nhân vô hướng `ScalarMultiplication` (vốn tốn ~300µs cho số 255-bit), chỉ thực hiện phép cộng trừ Jacobian `butterflyG1(&a[i], &a[i+m])`.
   - Tiết kiệm **382 phép nhân vô hướng $G_1$** trong mỗi lần chạy FK20.

2. **Dồn tỉ lệ $1/(2N)$ về trường vô hướng $\mathbb{F}_r$ (Pre-scaling in $\mathbb{F}_r$):**
   - Thay vì nhân vô hướng $2N = 256$ điểm $G_1$ sau khi IFFT, ta nhân trực tiếp $\hat{a}_k \times \frac{1}{2N} \pmod r$ ngay sau phép scalar FFT trong trường $\mathbb{F}_r$.
   - Phép nhân trên $\mathbb{F}_r$ chỉ tốn 15ns $\implies$ loại bỏ hoàn toàn **256 phép nhân vô hướng $G_1$**, tiết kiệm thêm ~50ms.

3. **Chuyển đổi Affine hàng loạt (Batch Jacobian to Affine via Montgomery Trick):**
   - Thay vì gọi `aff.FromJacobian` 128 lần (tốn 128 phép nghịch đảo modulo trong trường $\mathbb{F}_p$), sử dụng `bls12381.BatchJacobianToAffineG1(hG1)`.
   - Giảm từ 128 phép nghịch đảo xuống đúng **1 phép nghịch đảo duy nhất**.

4. **Tính Claimed Values bằng Scalar FFT $O(N \log N)$:**
   - Thay vì gọi phương pháp Horner $O(N^2)$ 128 lần, tính toàn bộ $P(\omega^i)$ cho cả 128 điểm bằng một lần gọi `domainN.FFT(poly, fft.DIF)` + `bitReverseScalars` (tốn <0.02ms).

5. **Xử lý song song đa luồng (Multi-core Parallel FFT & Piece Concurrency):**
   - Phân chia các tầng butterfly $m \ge 8$ và đệ quy DIF FFT qua các goroutine tận dụng toàn bộ số nhân CPU (`runtime.NumCPU()`).
   - Xử lý đồng thời $k_{\text{piece}} = 8$ đa thức mảnh của một cột bằng goroutines.

6. **Đồng bộ hóa điểm đánh giá (Evaluation Point Harmonization):**
   - `GnarkKZG` bổ sung `domain` cấp $N$ và phương thức `GetEvaluationPoint(row int) fr.Element`.
   - Nếu domain được thiết lập: trả về nghiệm đơn vị $\omega^{row}$.
   - Nếu domain không thiết lập: fallback về số nguyên $row$ để bảo toàn tính tương thích ngược 100% cho toàn bộ các unit test cũ.
   - Cập nhật `GnarkVerify` và `GnarkBatchVerify` dùng `GetEvaluationPoint(row)` để đồng bộ hoàn hảo với các proof sinh từ FK20.

---

## 4. Kết Quả Đo Lường & Đánh Giá Thực Nghiệm (Benchmark Results)

### 4.1 Micro-benchmark trên 1 đa thức ($N=128$, Intel Xeon E-2276G 12 cores)
| Thuật toán | Thời gian xử lý | Cấp phát bộ nhớ | Số allocs | Tăng tốc (Speedup) |
| :--- | :---: | :---: | :---: | :---: |
| **Naive Open (128 lần)** | ~73.38 ms | 5,580,273 B | 15,489 allocs | $1.0\times$ (Gốc) |
| **FK20 Optimized** | **24.64 ms** | **659,191 B** | **6,638 allocs** | **$3.0\times$ - $9.1\times$** |

*(Ghi chú: Trong unit test không cache warmup, Naive Open tốn ~207.2ms vs FK20 tốn ~22.7ms $\implies$ tăng tốc **9.12x**).*

### 4.2 End-to-End Integration Test (`GenerateColumnProofs` cho toàn bộ cột $N=128, K=8$)
- **Trước tối ưu (Naive Open $128 \times 8 = 1024$ proofs):** **~3,013 ms** (3.0 giây).
- **Sau tối ưu (FK20 song song 8 pieces):** **133.9 ms** (0.13 giây).
- **Hiệu quả cải thiện:** Tăng tốc **22.5 lần** ($\approx 95.6\%$ thời gian chờ của Bootstrap Node được cắt giảm).
- **Tính đúng đắn (Mathematical Correctness):**
  - Tất cả các proof sinh ra từ FK20 khớp 100% với giá trị `kzg.Open()`.
  - Store Node xác thực đơn lẻ (`Verify`) và xác thực lô (`BatchVerify`) đều thành công 100%.
  - Quá trình Recoding đa thế hệ trên Store Node (`RecodePieces`) duy trì tính đúng đắn và verify thành công.
