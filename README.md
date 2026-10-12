# Realtimechat

Aplikasi chat real-time menggunakan Go, Gorilla WebSocket, goroutines, channels, dan Hub dengan room terpisah.

## Fitur
- Halaman chat di `/`
- WebSocket di `/ws`
- Broadcast pesan hanya ke room yang sama
- Username dan room dapat dipilih dari browser
- Ping/pong untuk mendeteksi koneksi yang putus
- Batas ukuran frame 4 KiB dan teks maksimum 1.000 karakter
- Health check di `/healthz`
- Port dari environment variable `PORT`, default lokal `8080`
- HTML di-embed ke binary melalui `embed`

## Jalankan lokal
Perlu Go 1.22 atau lebih baru.

```sh
go mod tidy
go run .
```

Buka http://localhost:8080 di dua tab browser. Pilih room yang sama untuk saling berkirim pesan.

## Deploy ke Render
1. Hubungkan repository ini ke Render sebagai **Web Service**.
2. Runtime: **Go**.
3. Build Command: `go build -o app .`
4. Start Command: `./app`
5. Health Check Path (opsional): `/healthz`.
6. Deploy. Render menyediakan `PORT` otomatis; server bind ke `0.0.0.0:$PORT`.

## Catatan
Contoh ini belum memiliki login atau autentikasi. Username dipilih sendiri oleh browser, pesan tidak disimpan ke database, dan room belum memiliki otorisasi. Gunakan autentikasi, rate limiting, validasi identitas, dan penyimpanan pesan sebelum digunakan sebagai layanan chat publik. Hub ini berjalan dalam satu instance; untuk beberapa instance, tambahkan broker pub/sub.
