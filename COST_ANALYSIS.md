# Daily Cost Analysis - Viral Clips Pipeline

## Usage Patterns (Default Schedule)

Based on the daemon's default schedule:

| Service | Frequency | Per Run | Daily Total |
|---------|-----------|---------|-------------|
| **Apify Discovery** | Every 4 hours | 50 videos | 6 runs/day = 300 videos |
| **OpenAI Classification** | Every 30 min | ~20-30 videos | 48 runs/day = ~1,200 videos |
| **YouTube Uploads** | Every 6 hours | 1 video | 4 uploads/day |
| **Downloads** | Every 30 min | ~20 videos | 48 runs/day = ~960 videos |

---

## Cost Breakdown

### 1. Apify API (TikTok Scraping)

**Actor:** `clockworks/tiktok-scraper`  
**Pricing:** ~$0.25-0.50 per actor run (varies by usage)  
**Usage:**
- 6 discovery runs/day (every 4 hours)
- Each run fetches ~50 videos

**Estimated Cost:**
- 6 runs × $0.35/run = **$2.10/day**

*Note: Apify has a free tier with $5/month credit, so actual cost may be lower initially*

---

### 2. OpenAI API (Video Classification)

**Model:** GPT-4o-mini  
**Pricing (as of 2024):**
- Input: $0.15 per 1M tokens
- Output: $0.60 per 1M tokens

**Usage per video classification:**
- Input prompt: ~2,000 tokens (classification prompt + video metadata)
- Output: ~300 tokens (JSON response)
- Total: ~2,300 tokens per video

**Daily calculations:**
- Videos classified: ~1,200 videos/day (48 runs × ~25 videos avg)
- Total tokens: 1,200 × 2,300 = 2,760,000 tokens/day
- Input cost: 2.76M tokens × ($0.15/1M) = $0.414
- Output cost: 1,200 × 300 tokens × ($0.60/1M) = $0.216
- **Total: ~$0.63/day**

---

### 3. YouTube Data API

**Status:** FREE  
**Quota:** 10,000 units/day (default)  
**Usage:**
- Video upload: 1,600 units per upload
- 4 uploads/day = 6,400 units/day

**Cost:** $0/day (within free quota)

---

### 4. Infrastructure/Compute

**Status:** Running locally  
**Cost:** $0/day (uses your own hardware/internet)

---

## Total Daily Cost Summary

| Component | Daily Cost |
|-----------|------------|
| Apify API (TikTok scraping) | $2.10 |
| OpenAI API (Classification) | $0.63 |
| YouTube Data API | $0.00 |
| Infrastructure | $0.00 |
| **TOTAL** | **$2.73/day** |

---

## Monthly Cost Estimate

- **Per Day:** $2.73
- **Per Month (30 days):** $81.90
- **Per Year:** ~$996

---

## Cost Optimization Tips

1. **Apify:** 
   - Use free tier credits ($5/month) = ~14 free runs
   - Reduce discovery frequency if needed
   - Current: 6 runs/day → could go to 4 runs/day ($1.40/day)

2. **OpenAI:**
   - Already using cheapest model (GPT-4o-mini)
   - Could reduce classification frequency (30min → 1 hour) = $0.32/day
   - Pre-filtering already reduces API calls significantly

3. **Scaling:**
   - Costs scale linearly with video volume
   - 2x videos = ~2x costs
   - Most efficient at current throughput

---

## Breakdown by Activity

**Per Video Processed:**
- Discovery: $0.007/video (6 runs × 50 videos = 300 videos, $2.10/300)
- Classification: $0.0005/video ($0.63/1,200)
- Upload: $0/video (free)

**Per Uploaded Compilation:**
- Total pipeline cost per compilation: ~$0.68
- (Discovery + Classification + Processing for ~5-8 videos per compilation)

