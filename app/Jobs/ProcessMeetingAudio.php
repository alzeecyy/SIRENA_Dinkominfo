<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use App\Models\Notulensi;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;

class ProcessMeetingAudio implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public $tries = 1;
    public $timeout = 600;

    protected $notulensi;
    protected $secretaryId;
    protected $specificAudioPath;

    /**
     * Create a new job instance.
     */
    public function __construct(Notulensi $notulensi, $secretaryId, $specificAudioPath = null)
    {
        $this->notulensi = $notulensi;
        $this->secretaryId = $secretaryId;
        $this->specificAudioPath = $specificAudioPath;
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        @set_time_limit(0);
        @ini_set('memory_limit', '512M');

        try {
            Log::info("ProcessMeetingAudio job started for notulensi ID: " . $this->notulensi->id);
            
            // Refresh model from DB to get the latest audio_files array
            $this->notulensi->refresh();
            
            $agenda = $this->notulensi->agenda;
            $apiKey = env('GEMINI_API_KEY');

            $pythonPath = env('PYTHON_PATH', 'python');
            $scriptPath = base_path('transcribe_whisper_cpp.py');

            // Retrieve list of audio files to process
            $audioFiles = $this->notulensi->audio_files;
            if (empty($audioFiles) && $this->notulensi->audio_path) {
                $audioFiles = [
                    [
                        'name' => $this->notulensi->audio_name ?? 'Rekaman Audio',
                        'path' => $this->notulensi->audio_path
                    ]
                ];
            }

            $transcriptBlocks = [];

            if (empty($audioFiles)) {
                if (!empty(trim($this->notulensi->transkrip_raw ?? ''))) {
                    Log::info("ProcessMeetingAudio: No audio files found, analyzing typed text transcript directly.");
                    $transcriptBlocks[] = trim($this->notulensi->transkrip_raw);
                } else {
                    Log::warning("ProcessMeetingAudio: Neither audio files nor transcript text found to process.");
                    $this->notulensi->update([
                        'is_transcribing' => false,
                        'transkrip_error' => 'Silakan unggah berkas audio rapat ATAU ketik/tempel catatan pada bidang transkrip terlebih dahulu.'
                    ]);
                    return;
                }
            } else {

            foreach ($audioFiles as $index => $audioItem) {
                $audioPath = $audioItem['path'] ?? null;
                $audioName = $audioItem['name'] ?? ('Rekaman #' . ($index + 1));
                
                if (!$audioPath) continue;

                $audioFile = str_replace(['/', '\\'], DIRECTORY_SEPARATOR, Storage::disk('public')->path($audioPath));
                $scriptPath = str_replace(['/', '\\'], DIRECTORY_SEPARATOR, base_path('transcribe_whisper_cpp.py'));

                if (!file_exists($audioFile)) {
                    Log::error("ProcessMeetingAudio: Audio file not found at " . $audioFile);
                    continue;
                }

                try {
                    $transcribedText = null;

                    // 1. Primary STT Engine: Gemini Multimodal STT (Fast cloud transcription for files <= 20MB)
                    if ($apiKey && filesize($audioFile) <= 20 * 1024 * 1024) {
                        try {
                            Log::info("ProcessMeetingAudio: Transcribing {$audioName} via Primary Engine (Gemini Multimodal STT)...");
                            $mimeType = 'audio/mp3';
                            $ext = strtolower(pathinfo($audioFile, PATHINFO_EXTENSION));
                            if ($ext === 'wav') $mimeType = 'audio/wav';
                            if ($ext === 'm4a') $mimeType = 'audio/m4a';
                            if ($ext === 'ogg') $mimeType = 'audio/ogg';
                            if ($ext === 'flac') $mimeType = 'audio/flac';
                            if ($ext === 'aac') $mimeType = 'audio/aac';
                            if ($ext === 'webm') $mimeType = 'audio/webm';

                            $audioBase64 = base64_encode(file_get_contents($audioFile));

                            $sttResponse = Http::withoutVerifying()->connectTimeout(5)->timeout((int) env('GEMINI_STT_TIMEOUT', 30))->post("https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent?key=" . $apiKey, [
                                'contents' => [
                                    [
                                        'parts' => [
                                            [
                                                'inlineData' => [
                                                    'mimeType' => $mimeType,
                                                    'data' => $audioBase64
                                                ]
                                            ],
                                            [
                                                'text' => "Transkripsikan percakapan suara dari audio rapat ini secara lengkap, rinci, dan akurat ke dalam Bahasa Indonesia baku. Tuliskan HANYA teks transkrip percakapan suara tanpa kata pengantar atau penjelasan tambahan. DILARANG mencantumkan label pembicara seperti 'Pria:', 'Wanita:', 'Speaker 1:', atau identitas jenis kelamin/gender apapun. Tuliskan langsung isi percakapannya saja."
                                            ]
                                        ]
                                    ]
                                ]
                            ]);

                            if ($sttResponse->successful()) {
                                $sttResult = $sttResponse->json();
                                $sttText = $sttResult['candidates'][0]['content']['parts'][0]['text'] ?? null;
                                if (!empty($sttText)) {
                                    $transcribedText = trim($sttText);
                                    Log::info("ProcessMeetingAudio: Gemini Multimodal STT successful for {$audioName}.");
                                }
                            } else {
                                Log::error("ProcessMeetingAudio: Gemini Multimodal STT failed with status " . $sttResponse->status() . ": " . $sttResponse->body());
                            }
                        } catch (\Exception $e) {
                            Log::error("ProcessMeetingAudio: Gemini Multimodal STT exception: " . $e->getMessage());
                        }
                    }

                    // 2. Fallback STT Engine: Local Whisper.cpp (If Gemini STT failed, offline, or file > 20MB)
                    if (empty($transcribedText)) {
                        Log::info("ProcessMeetingAudio: Transcribing {$audioName} via Fallback Engine (Local Whisper.cpp)...");
                        
                        $cmd = '"' . $pythonPath . '" "' . $scriptPath . '" "' . $audioFile . '"';
                        $descriptors = [
                            0 => ["pipe", "r"],
                            1 => ["pipe", "w"],
                            2 => ["pipe", "w"],
                        ];

                        $process = proc_open($cmd, $descriptors, $pipes, base_path());
                        $output = '';
                        $stderr = '';

                        if (is_resource($process)) {
                            fclose($pipes[0]);
                            $output = stream_get_contents($pipes[1]);
                            $stderr = stream_get_contents($pipes[2]);
                            fclose($pipes[1]);
                            fclose($pipes[2]);
                            $returnCode = proc_close($process);
                        }

                        if ($output) {
                            $data = json_decode($output, true);
                            if (json_last_error() === JSON_ERROR_NONE && isset($data['status']) && $data['status'] === 'success') {
                                $transcribedText = self::cleanTranscriptText($data['text'] ?? '');
                            } else {
                                Log::error("ProcessMeetingAudio: Whisper error on file " . $audioName . ": Output: " . $output . " Stderr: " . $stderr);
                            }
                        } else {
                            Log::error("ProcessMeetingAudio: Empty output from Python process. Stderr: " . $stderr);
                        }
                    }

                    if (!empty($transcribedText)) {
                        $cleanedText = self::cleanTranscriptText($transcribedText);
                        if (!empty($cleanedText)) {
                            if (count($audioFiles) > 1) {
                                $transcriptBlocks[] = "📌 BAGIAN REKAMAN " . ($index + 1) . ": " . mb_strtoupper($audioName) . "\n" . str_repeat("—", 50) . "\n" . $cleanedText;
                            } else {
                                $transcriptBlocks[] = $cleanedText;
                            }
                        }
                    }
                } catch (\Exception $e) {
                    Log::error("ProcessMeetingAudio exception for " . $audioName . ": " . $e->getMessage());
                }
            }
            }

            if (empty($transcriptBlocks)) {
                Log::error("ProcessMeetingAudio: All audio transcriptions failed or returned empty.");
                $this->notulensi->update([
                    'is_transcribing' => false,
                    'transkrip_error' => 'Transkripsi gagal: Tidak dapat memproses berkas audio. Pastikan berkas audio valid dan memuat suara percakapan.',
                ]);
                return;
            }

            // Combine formatted transcriptions
            $combinedTranscript = implode("\n\n\n", $transcriptBlocks);

            $summarized = false;

            // Check if combined transcript is too short to summarize
            $textLen = strlen(trim($combinedTranscript));
            if ($textLen < 15) {
                $this->notulensi->update([
                    'transkrip_raw' => $combinedTranscript,
                    'ringkasan' => "Transkripsi selesai. Rekaman audio terlalu singkat/pendek untuk dianalisis secara lengkap oleh AI.",
                    'pembahasan' => null,
                    'keputusan' => null,
                    'kesimpulan' => null,
                    'last_edited_by_id' => $this->secretaryId,
                    'status' => 'draft'
                ]);
                $summarized = true;
                Log::info("ProcessMeetingAudio: Transcript too short ({$textLen} chars). Skipped LLM summarization.");
            }

            $prompt = self::buildSummaryPrompt($combinedTranscript);

            // 1. Primary LLM Engine: Google Gemini API (Super fast response)
            if (!$summarized && $apiKey) {
                try {
                    Log::info("ProcessMeetingAudio: calling Primary LLM Engine (Gemini API 3.6 Flash) to summarize...");
                    $url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent?key=" . $apiKey;
                    $responseSummary = Http::withoutVerifying()->connectTimeout(5)->timeout((int) env('GEMINI_TIMEOUT', 10))->post($url, [
                        'contents' => [
                            [
                                'parts' => [
                                    [
                                        'text' => $prompt
                                    ]
                                ]
                            ]
                        ],
                        'generationConfig' => [
                            'temperature' => 0.1,
                            'topP' => 0.8,
                        ]
                    ]);
                    
                    if ($responseSummary->successful()) {
                        $sumResult = $responseSummary->json();
                        $sumText = $sumResult['candidates'][0]['content']['parts'][0]['text'] ?? null;
                        if ($sumText) {
                            $sumText = trim(preg_replace('/```(?:markdown)?/i', '', $sumText));
                            $sumText = self::cleanRepetitiveLoops($sumText);
                            $sumText = self::fixMarkdownFormat($sumText);
                            $this->notulensi->update([
                                'transkrip_raw' => $combinedTranscript,
                                'ringkasan' => $sumText,
                                'pembahasan' => null,
                                'keputusan' => null,
                                'kesimpulan' => null,
                                'transkrip_error' => null,
                                'last_edited_by_id' => $this->secretaryId,
                                'status' => 'draft'
                            ]);
                            $summarized = true;
                            Log::info("ProcessMeetingAudio completed and summarized via Primary LLM Engine (Gemini API) successfully.");
                        }
                    } else {
                        Log::error("ProcessMeetingAudio: Primary LLM (Gemini) failed with status " . $responseSummary->status() . ": " . $responseSummary->body());
                    }
                } catch (\Exception $e) {
                    Log::error("ProcessMeetingAudio exception during Primary LLM (Gemini) call: " . $e->getMessage());
                }
            }

            // 2. Fallback LLM Engine: Custom OpenAI-compatible API / Local Qwen 2.5 (If offline or Gemini failed)
            $llmApiBase = env('LLM_API_BASE');
            $llmApiKey = env('LLM_API_KEY', 'none');
            $llmModel = env('LLM_MODEL', 'qwen2.5:3b');

            if (!$summarized && $llmApiBase) {
                try {
                    Log::info("ProcessMeetingAudio: calling Fallback LLM Engine ({$llmApiBase}) with model: {$llmModel}...");
                    $url = rtrim($llmApiBase, '/') . '/chat/completions';
                    $response = Http::timeout(120)->withHeaders([
                        'Authorization' => 'Bearer ' . $llmApiKey,
                        'Content-Type' => 'application/json'
                    ])->post($url, [
                        'model' => $llmModel,
                        'temperature' => 0.1,
                        'top_p' => 0.8,
                        'frequency_penalty' => 0.3,
                        'presence_penalty' => 0.2,
                        'max_tokens' => 3000,
                        'messages' => [
                            [
                                'role' => 'system',
                                'content' => 'Kamu adalah asisten sekretaris notulensi rapat yang sangat teliti, objektif, dan faktual. Kamu dilarang keras mengarang atau menambahkan informasi fiktif yang tidak ada di transkrip rapat.'
                            ],
                            [
                                'role' => 'user',
                                'content' => $prompt
                            ]
                        ]
                    ]);

                    if ($response->successful()) {
                        $resJson = $response->json();
                        $sumText = $resJson['choices'][0]['message']['content'] ?? null;
                        if ($sumText) {
                            $sumText = trim(preg_replace('/```(?:markdown)?/i', '', $sumText));
                            $sumText = self::cleanRepetitiveLoops($sumText);
                            if (self::isRepetitiveLoop($sumText)) {
                                Log::warning("ProcessMeetingAudio: Fallback LLM ({$llmModel}) output detected as repetition loop. Discarding.");
                            } else {
                                $sumText = self::fixMarkdownFormat($sumText);
                                $this->notulensi->update([
                                    'transkrip_raw' => $combinedTranscript,
                                    'ringkasan' => $sumText,
                                    'pembahasan' => null,
                                    'keputusan' => null,
                                    'kesimpulan' => null,
                                    'transkrip_error' => null,
                                    'last_edited_by_id' => $this->secretaryId,
                                    'status' => 'draft'
                                ]);
                                $summarized = true;
                                Log::info("ProcessMeetingAudio completed and summarized via Fallback LLM Engine ({$llmModel}) successfully.");
                            }
                        }
                    } else {
                        Log::error("ProcessMeetingAudio: Fallback LLM API request failed with status: " . $response->status() . " Body: " . $response->body());
                    }
                } catch (\Exception $e) {
                    Log::error("ProcessMeetingAudio exception during Fallback LLM API call: " . $e->getMessage());
                }
            }

            // If offline or Gemini failed, run smart local heuristic summarizer
            if (!$summarized) {
                Log::info("ProcessMeetingAudio: Generating local heuristic summary...");
                
                $lines = preg_split('/[.!?\n]+/', $combinedTranscript);
                $lines = array_map('trim', $lines);
                $lines = array_filter($lines, function($l) { 
                    return strlen($l) > 3; 
                });
                
                $textLen = strlen(trim($combinedTranscript));
                
                if ($textLen < 120) {
                    // Audio transcript is too short or simple greetings
                    $ringkasan = trim($combinedTranscript);
                    if (empty($ringkasan)) {
                        $ringkasan = "Belum ada ringkasan hasil rapat.";
                    }
                    
                    $pembahasan = "1. Rekaman audio terlalu singkat untuk diidentifikasi poin pembahasannya.";
                    $keputusan = "1. Tidak ada keputusan yang dapat diidentifikasi dari rekaman singkat ini.";
                    $kesimpulan = "Transkripsi selesai namun audio terlalu pendek untuk dianalisis secara lengkap.";
                } else {
                    // Transcript has actual content. Build a comprehensive summary paragraph
                    // covering the beginning, middle, and end of the transcript.
                    $cleanLines = array_values(array_filter($lines, function($l) {
                        return strlen($l) > 10;
                    }));
                    
                    $totalLines = count($cleanLines);
                    $summarySentences = [];
                    
                    if ($totalLines <= 5) {
                        $summarySentences = $cleanLines;
                    } else {
                        $summarySentences[] = $cleanLines[0];
                        $summarySentences[] = $cleanLines[1];
                        
                        $mid = floor($totalLines / 2);
                        $summarySentences[] = $cleanLines[$mid];
                        if ($totalLines > 6) {
                            $summarySentences[] = $cleanLines[$mid + 1];
                        }
                        
                        $summarySentences[] = $cleanLines[$totalLines - 1];
                    }
                    
                    $summarySentences = array_unique($summarySentences);
                    $ringkasan = "Rapat ini membahas jalannya agenda koordinasi berdasarkan rekaman audio rapat yang berhasil ditranskrip secara offline. " . implode(". ", $summarySentences) . ".";
                    $ringkasan = preg_replace('/\.+/', '.', $ringkasan);
                    $ringkasan = str_replace('..', '.', $ringkasan);
                    
                    $pembahasanLines = array_slice($lines, 0, 4);
                    if (empty($pembahasanLines)) {
                        $pembahasanLines[] = "Pembahasan sesuai dengan rekaman suara yang diunggah.";
                    }
                    
                    $keputusanLines = [];
                    foreach ($lines as $line) {
                        if (preg_match('/(setuju|sepakat|putus|pilih|mulai|harus|tindak|akan|laku|evaluasi|bahas|rapat)/i', $line)) {
                            $keputusanLines[] = $line;
                        }
                    }
                    $keputusanLines = array_slice($keputusanLines, 0, 3);
                    if (empty($keputusanLines)) {
                        $keputusanLines[] = "Menyetujui penyelarasan progres koordinasi yang telah dicatat.";
                        $keputusanLines[] = "Menyepakati tindak lanjut penyelesaian poin pembahasan.";
                    }
                    
                    $pembahasan = "";
                    foreach ($pembahasanLines as $idx => $line) {
                        $pembahasan .= ($idx + 1) . ". " . $line . "\n";
                    }
                    $pembahasan = rtrim($pembahasan);
                    
                    $keputusan = "";
                    foreach ($keputusanLines as $idx => $line) {
                        $keputusan .= ($idx + 1) . ". " . $line . "\n";
                    }
                    $keputusan = rtrim($keputusan);
                    
                    $kesimpulan = "Poin utama rapat telah dicatat berdasarkan transkrip suara.";
                }
                
                $this->notulensi->update([
                    'transkrip_raw' => $combinedTranscript,
                    'ringkasan' => $ringkasan,
                    'pembahasan' => $pembahasan,
                    'keputusan' => $keputusan,
                    'kesimpulan' => $kesimpulan,
                    'transkrip_error' => null,
                    'last_edited_by_id' => $this->secretaryId,
                    'status' => 'draft'
                ]);
                Log::info("ProcessMeetingAudio completed and summarized via Local Heuristic successfully.");
            }
        } finally {
            $this->notulensi->update([
                'is_transcribing' => false
            ]);
            Log::info("ProcessMeetingAudio finally: set is_transcribing to false.");
        }
    }

    public static function cleanTranscriptText(string $text): string
    {
        if (empty($text)) {
            return '';
        }

        // Hapus hanya suara hening/interjeksi murni tanpa makna (tanpa mengganggu kata Bahasa Indonesia baku seperti 'ya', 'kan', 'gitu')
        $rawFillers = [
            '/\bhehe\b/i', '/\bhihi\b/i', '/\buh\b/i', '/\bum\b/i', '/\beh-eh\b/i'
        ];
        $text = preg_replace($rawFillers, '', $text);

        // Rapikan spasi, koma, dan titik berlebih tanpa menghapus kata ulang (seperti 'sama-sama', 'bapak-bapak', 'kira-kira') atau huruf tunggal
        $text = preg_replace('/\s+/', ' ', $text);
        $text = preg_replace('/\s*,\s*/', ', ', $text);
        $text = preg_replace('/\s*\.\s*/', '. ', $text);
        $text = preg_replace('/,\s*\./', '.', $text);
        $text = preg_replace('/\.{2,}/', '.', $text);
        $text = preg_replace('/,{2,}/', ',', $text);
        
        $sentences = preg_split('/([.!?]\s+)/', $text, -1, PREG_SPLIT_DELIM_CAPTURE);
        $text = '';
        foreach ($sentences as $i => $s) {
            if ($i % 2 === 0) {
                $text .= ucfirst(trim($s));
            } else {
                $text .= $s;
            }
        }

        return trim($text);
    }

    /**
     * Bangun prompt summarization terstandar untuk semua LLM (Primary Gemini & Fallback Lokal).
     */
    public static function buildSummaryPrompt(string $transcript): string
    {
        return "Kamu adalah sekretaris rapat profesional. Rangkum teks transkrip berikut menjadi notulensi Markdown yang rapi, faktual, dan mudah dibaca.\n\n" .
               "ATURAN WAJIB:\n" .
               "1. HANYA gunakan fakta dari transkrip. DILARANG mengarang PIC, tanggal, keputusan, contoh fiktif, atau mengutip '(Teks asli: ...)'.\n" .
               "2. Langsung mulai dengan paragraf ringkasan pembuka di baris pertama dari sudut pandang notulensi rapat (misalnya 'Rapat ini membahas...' atau langsung uraikan topiknya). DILARANG menulis 'Teks ini membahas...', 'Transkrip ini...', kalimat pengantar ('Berikut adalah...'), salam penutup, emoji, garis '---', atau judul '#' di paling atas.\n" .
               "3. Wajib cantumkan semua 4 bagian berikut secara berurutan:\n" .
               "   - Bagian pembuka: langsung tulis 1 paragraf ringkasan umum (3-5 kalimat) mengenai inti pembahasan rapat.\n" .
               "   - **A. POIN-POIN PEMBAHASAN UTAMA**: tulis daftar bernomor bulat 1., 2., 3., dst. (bukan 1.0). Pada setiap butir, tuliskan frasa inti pembahasannya secara tebal diikuti tanda titik dua lalu penjelasan lengkapnya dari transkrip.\n" .
               "   - **B. KEPUTUSAN & TINDAK LANJUT**: tulis keputusan yang disepakati jika ada; jika tidak ada, tulis persis: Belum ada keputusan atau tindak lanjut spesifik yang ditetapkan dalam percakapan ini.\n" .
               "   - **C. CATATAN & PERLU KLARIFIKASI**: periksa transkrip untuk mencari kata yang typo, ejaan tidak baku (misalnya serapan yang salah eja), atau kata salah dengar transkripsi suara STT, lalu tulis maksimal 5 butir dengan tanda '-' beserta saran ejaan/kata yang benar. Jangan mengulang kata yang sama. Jika semua sudah baku dan jelas, tulis: Tidak ada.\n\n" .
               "Teks Sumber Transkrip:\n\n" . $transcript;
    }

    /**
     * Perbaiki format Markdown output LLM agar rapi dan selalu lengkap:
     * - Hapus heading '# ...' di awal dokumen yang tidak perlu
     * - Pastikan header section **A. / **B. / **C. terpisah rapi dengan baris kosong
     * - Pastikan item bernomor (1. 2. 3.) mulai di baris baru tanpa spasi di awal
     * - Pastikan bagian B dan C selalu ada jika terpotong oleh model kecil
     */
    public static function fixMarkdownFormat(string $text): string
    {
        if (empty($text)) return '';

        // Normalisasi line endings
        $text = str_replace(["\r\n", "\r"], "\n", $text);

        // Hapus garis pemisah '---' / '***' atau heading '#' di awal dokumen
        $text = preg_replace('/^\s*(?:[-*_]{3,}|#{1,3}\s*[^\n]*(?:notulensi|ringkasan|risalah)[^\n]*)\s*\n+/iu', '', $text);

        // Normalisasi penomoran desimal seperti "1.0 ", "2.0 " menjadi "1. ", "2. "
        $text = preg_replace('/(^|\n)[ \t]*(\d{1,2})\.0([ \t]+)/u', '$1$2.$3', $text);

        // Normalisasi header Markdown seperti "## A. POIN..." menjadi "**A. POIN...**"
        $text = preg_replace('/^[ \t]*#{1,4}[ \t]+([A-C]\.\s+[^\n#*]+)\s*$/mu', '**$1**', $text);

        // Pastikan section header **A. / **B. / **C. berdiri di baris sendiri dengan jarak 2 newline sebelum & sesudahnya
        $text = preg_replace('/\s*(\*\*[A-C]\.\s+[A-Z][^\n*]+\*\*)\s*/u', "\n\n$1\n\n", $text);

        // Pastikan item bernomor (1. 2. 3. dst) setelah tanda baca akhir kalimat atau baris sebelumnya mulai di baris baru tanpa spasi depan
        $text = preg_replace('/(?<=[.!?:)"\'])[ \t]+(\d{1,2}\.\s+)/u', "\n$1", $text);

        // Hapus spasi/tab berlebih di awal baris sebelum angka bernomor (" 2. " -> "2. ")
        $text = preg_replace('/^[ \t]+(\d{1,2}\.\s+)/m', '$1', $text);

        // Pastikan item bullet ("- kata") setelah kalimat baru mulai di baris baru tanpa spasi depan
        $text = preg_replace('/(?<=[.!?:)"\'])[ \t]+(-\s+)/u', "\n$1", $text);
        $text = preg_replace('/^[ \t]+(-\s+)/m', '$1', $text);

        // Safety net: pastikan bagian B dan C selalu ada jika model kecil memotongnya
        if (!preg_match('/\*\*B\.\s+KEPUTUSAN/i', $text)) {
            $text = rtrim($text) . "\n\n**B. KEPUTUSAN & TINDAK LANJUT**\n\nBelum ada keputusan atau tindak lanjut spesifik yang ditetapkan dalam percakapan ini.";
        }
        if (!preg_match('/\*\*C\.\s+CATATAN/i', $text)) {
            $text = rtrim($text) . "\n\n**C. CATATAN & PERLU KLARIFIKASI**\n\nTidak ada.";
        }

        // Rapikan: maksimal 2 baris kosong berturut-turut
        $text = preg_replace('/\n{3,}/', "\n\n", $text);

        return trim($text);
    }

    /**
     * Deteksi apakah teks mengandung repetition loop parah (LLM hallucination).
     * Return true jika teks tidak layak dipakai.
     */
    public static function isRepetitiveLoop(string $text): bool
    {
        if (empty($text)) return false;

        // 1. Cek repetisi per baris (menangkap loop seperti "- PostgreSQL" berulang kali)
        $lines = array_values(array_filter(array_map('trim', explode("\n", $text)), fn($l) => strlen($l) > 3));
        $totalLines = count($lines);
        if ($totalLines >= 6) {
            $lineFreq = array_count_values($lines);
            $maxLineRepeat = max($lineFreq);
            if ($maxLineRepeat >= 5 || ($maxLineRepeat / $totalLines) > 0.3) {
                return true;
            }
        }

        // 2. Cek repetisi per kalimat
        $sentences = preg_split('/(?<=[.!?])\s+/', trim($text));
        $sentences = array_filter(array_map('trim', $sentences), fn($s) => strlen($s) > 5);
        $sentences = array_values($sentences);

        $total = count($sentences);
        if ($total < 5) return false;

        $freq = array_count_values($sentences);
        $maxRepeat = max($freq);

        if ($maxRepeat / $total > 0.3) return true;

        if ($total > 20) {
            $tail = array_slice($sentences, -20);
            $tailFreq = array_count_values($tail);
            if (max($tailFreq) >= 10) return true;
        }

        return false;
    }

    public static function cleanRepetitiveLoops(string $text): string
    {
        if (empty($text)) return '';

        // 1. Deduplikasi per baris sambil mempertahankan struktur newline
        $rawLines = explode("\n", str_replace(["\r\n", "\r"], "\n", $text));
        $seenLines = [];
        $dedupedLines = [];
        foreach ($rawLines as $line) {
            $trimmed = trim($line);
            if ($trimmed === '') {
                $dedupedLines[] = '';
                continue;
            }
            $key = mb_strtolower($trimmed);
            $seenLines[$key] = ($seenLines[$key] ?? 0) + 1;
            if ($seenLines[$key] <= 1) {
                $dedupedLines[] = $line;
            }
        }
        $text = implode("\n", $dedupedLines);

        // 2. Hapus pengulangan kalimat pada baris yang sama tanpa merusak newline
        $finalLines = [];
        foreach (explode("\n", $text) as $line) {
            $sentences = preg_split('/(?<=[.!?])[ \t]+/', trim($line));
            $seen = [];
            $deduped = [];
            foreach ($sentences as $sentence) {
                $key = mb_strtolower(trim($sentence));
                if ($key === '') continue;
                $seen[$key] = ($seen[$key] ?? 0) + 1;
                if ($seen[$key] <= 2) {
                    $deduped[] = $sentence;
                }
            }
            $finalLines[] = implode(' ', $deduped);
        }
        $text = implode("\n", $finalLines);

        // Hapus pengulangan frasa berulang (misal "untuk mempertimbangkan untuk mempertimbangkan...")
        $cleaned = preg_replace('/(?:\b(\w+(?:\s+\w+){0,9})\b)(?:\s+\1){2,}/iu', '$1', $text);
        $cleaned = preg_replace('/(\b\w+\s+\w+\b)(?:\s+\1){2,}/iu', '$1', $cleaned);

        return trim($cleaned);
    }
}
