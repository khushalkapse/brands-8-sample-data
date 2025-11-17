# LTK Daily Trigger & Post Ingestion - Deep Dive Explanation

## 📋 Table of Contents
1. [Complete Flow Overview](#complete-flow-overview)
2. [Data Storage Locations](#data-storage-locations)
3. [Deduplication Checks](#deduplication-checks)
4. [Daily Trigger Flow (Detailed)](#daily-trigger-flow-detailed)
5. [Post Ingestion Flow (Detailed)](#post-ingestion-flow-detailed)
6. [Error Handling & Dead Letter Queues](#error-handling--dead-letter-queues)

---

## 🎯 Complete Flow Overview

Think of this system like a **factory assembly line**:
1. **Daily Trigger** = Finding which workers (influencers) need to work today
2. **Scraper** = Workers collecting raw materials (posts) from ShopLTK
3. **Post Processor** = Quality control - enriching and cleaning the materials
4. **Data Modelling** = Packaging materials into finished products (Brands, Products, Looks, Posts)

---

## 💾 Data Storage Locations

### **DynamoDB Tables** (Main Database)

#### 1. **validation_table** 
- **What it stores**: User/influencer information
- **Key fields**: 
  - `user_id` (primary key)
  - `ltk_url` - The ShopLTK profile URL
  - `ltk_fetch_trigger` - Flag to track if scraping was triggered
  - `access_token` - For Instagram (if applicable)
- **File location**: `cdk/ask_velvee_service/service_stack.py` (line 150)
- **Used by**: Platform Validation Lambda, LTK Validation Lambda

#### 2. **DataModellingTable** (Main Post Storage)
- **Full name**: `ask-velvee-service-dev-DataModellingFinalTable`
- **What it stores**: Enriched posts with all product details
- **Key fields**:
  - `id` (primary key) - The post ID
  - `user_id` - Which influencer created this post
  - `caption` - Post description
  - `post_urls` - Array of image/video URLs (CDN links)
  - `products` - Array of product objects with:
    - `product_id`, `brand_id`, `product_affiliate_link`
    - `price`, `description`, `s3_media_url` (product images)
  - `platform` - "LTK" or "Instagram"
  - `date_timestamp` - When post was created
- **File location**: `cdk/ask_velvee_service/service_stack.py` (line 228)
- **Has DynamoDB Stream**: YES (triggers Data Modelling Lambda)
- **Used by**: Post Processor Worker (writes), Data Modelling Lambda (reads)

#### 3. **DataModellingProcessedTable**
- **What it stores**: Processed data for MODIFY events (backup)
- **Key fields**:
  - `id` (primary key) - Post ID
  - `processed_data` - JSON with all created brands/products/looks/posts
- **File location**: `cdk/ask_velvee_service/service_stack.py` (line 237)
- **Used by**: Data Modelling Lambda (saves processed data here)

#### 4. **AffiliateTable**
- **What it stores**: Affiliate links for products
- **Key fields**:
  - `influencer_id` (partition key)
  - `product_id` (sort key)
  - `affiliate_link` - The actual affiliate URL
- **File location**: `cdk/ask_velvee_service/service_stack.py` (line 1498)
- **Used by**: Post Processor Worker (writes affiliate links here)

#### 5. **BrandMappingTable**
- **What it stores**: Brand name to brand ID mappings
- **Used by**: Post Processor Worker (to avoid creating duplicate brands)

#### 6. **ProductUploadQueue**
- **What it stores**: Failed product image uploads (for retry)
- **Used by**: Post Processor Worker (when product images fail to upload)

### **S3 Buckets** (File Storage)

#### 1. **ASK_VELVEE_RAG_BUCKET_SANDBOX_IA**
- **What it stores**: 
  - Post images/videos (downloaded from ShopLTK)
  - Product images (extracted from posts)
- **File location**: `ltkscraper-code/post_processor_sqs_worker.py` (line 218)
- **Upload function**: `ltkscraper-code/utils/s3Interact.py` - `upload_to_S3()`
- **Format**: Images stored as `.jpg`, `.png`, `.webp`, videos as `.mp4`

#### 2. **InstagramDataBucket**
- **What it stores**: Instagram media files
- **File location**: `cdk/ask_velvee_service/service_stack.py` (line 135)
- **Used by**: Instagram data handler

### **Cloudflare CDN** (Fast Image Delivery)

- **What it stores**: Optimized images for fast delivery
- **Upload function**: `ltkscraper-code/utils/cdn_uploader.py` - `upload_to_cloudflare_image()`
- **Used by**: Post Processor Worker (uploads images to both S3 and Cloudflare)
- **Result**: Images get both `s3_url` and `cdn_url` in the post data

### **Elasticsearch** (Search Index)

- **What it stores**: Influencer profiles for searching
- **Index name**: `influencer_index_{ENV_INDEX}` (e.g., `influencer_index_beta`)
- **Used by**: Daily Trigger Lambda (searches for influencers with LTK URLs)
- **File location**: `lambda_functions/trigger_ltk_for_new_posts.py` (line 23)

### **SQS Queues** (Message Queues)

#### 1. **ScraperSQS**
- **What it stores**: Messages with `user_id` and `ltk_url`
- **Purpose**: Queue of influencers waiting to be scraped
- **File location**: `cdk/ask_velvee_service/service_stack.py` (line 123)
- **Dead Letter Queue**: `ScraperSQS_DLQ` (failed messages go here)

#### 2. **PostProcessorSQS**
- **What it stores**: Raw post data from scraper
- **Message format**: JSON with `post_id`, `caption`, `image_url`, `user_id`, etc.
- **Purpose**: Queue of posts waiting to be enriched
- **File location**: `cdk/ask_velvee_service/service_stack.py` (line 600)

#### 3. **DataModellingDLQ**
- **What it stores**: Failed data modelling attempts
- **Purpose**: When Data Modelling Lambda fails, errors go here

---

## 🔍 Deduplication Checks

### **Level 1: Scraper Level (Before Sending to Post Processor)**

**Location**: `ltkscraper-code/ltk_initial_scraper_worker.py`

**Function**: `get_existing_post_ids()` (line 309)

**How it works**:
1. Takes a list of post IDs from ShopLTK API
2. Checks DynamoDB `DataModellingTable` in batches of 100
3. Returns set of post IDs that already exist
4. Only NEW posts (not in the set) are sent to Post Processor SQS

**Code snippet**:
```python
existing_post_ids = self.get_existing_post_ids(post_ids)

for pid in new_post_ids:
    if pid in existing_post_ids:
        logger.info(f'🔁 Post {pid} already exists. Skipping.')
        continue  # Skip this post
```

**What it checks**: 
- Post ID from ShopLTK (`objectID` field)
- Checks against `DataModellingTable` using `id` field

**File location**: `ltkscraper-code/ltk_initial_scraper_worker.py` (lines 309-340, 375-377)

---

### **Level 2: Post Processor Level (Before Saving to DynamoDB)**

**Location**: `ltkscraper-code/post_processor_sqs_worker.py`

**Current status**: ⚠️ **NO EXPLICIT DEDUP CHECK** at this level

**Why**: The scraper already filtered duplicates, so Post Processor assumes all messages are new.

**However**: DynamoDB `put_item()` will **overwrite** if same `id` exists (no error thrown).

**File location**: `ltkscraper-code/post_processor_sqs_worker.py` (line 1327)

---

### **Level 3: Data Modelling Level (Before Creating Entities)**

**Location**: `service/handlers/handler_data_modelling.py`

**For INSERT events** (line 1229):
- **No deduplication check** - assumes post is new
- Creates Brands, Products, Looks, Posts via API
- API endpoints handle their own deduplication

**For MODIFY events** (line 1152):
- Retrieves processed data from `DataModellingProcessedTable`
- Re-creates all entities (Brands, Products, Looks, Posts)
- This is for **updates**, not deduplication

**File location**: `service/handlers/handler_data_modelling.py` (lines 1146-1750)

---

### **Level 4: API Level (Final Deduplication)**

**Location**: API endpoints (external service)

**Endpoints**:
- `https://endpoints.velvee.com/brand/item`
- `https://endpoints.velvee.com/product/item`
- `https://endpoints.velvee.com/look/item`
- `https://endpoints.velvee.com/post/item`

**How it works**: 
- APIs check if entity with same `id` already exists
- If exists: Updates the entity
- If not: Creates new entity

**File location**: `service/handlers/handler_data_modelling.py` (lines 760-971)

---

## 🔄 Daily Trigger Flow (Detailed)

### **Step 1: AWS EventBridge Scheduled Rule**

**What happens**: Every day at **midnight UTC (00:00)**, AWS EventBridge triggers a Lambda function.

**Configuration**:
- **File**: `cdk/ask_velvee_service/service_stack.py` (line 740-751)
- **Schedule**: `cron(minute='0', hour='0')` - Daily at midnight
- **Target**: `TriggerLTKValidationLambda`

**Code**:
```python
daily_trigger_rule = events.Rule(
    'DailyLTKTriggerRule',
    schedule=events.Schedule.cron(minute='0', hour='0')
)
```

---

### **Step 2: Trigger LTK Lambda Function**

**File**: `lambda_functions/trigger_ltk_for_new_posts.py`

**What it does**:
1. **Connects to Elasticsearch** (line 28-38)
   - Uses API key authentication
   - Index: `influencer_index_{ENV_INDEX}`

2. **Searches for influencers with LTK URLs** (line 188-197)
   - Tries 3 different query strategies:
     - **Preferred**: Nested affiliate URLs query
     - **Fallback 1**: Object/flat affiliate URLs query
     - **Fallback 2**: Legacy `ltk_handles` query
   - Uses Elasticsearch scroll API for pagination (500 influencers per batch)

3. **For each influencer found** (line 221-247):
   - Extracts `user_id` and `ltk_url` from Elasticsearch result
   - Sends POST request to `LTK_VALIDATION_URL` with payload:
     ```json
     {
       "user_id": "...",
       "ltk_url": "https://www.shopltk.com/explore/username"
     }
     ```
   - Retries up to 3 times if request fails

4. **Scrolls through all results** (line 217-273)
   - Processes in batches of 500
   - Continues until no more results

**Key functions**:
- `lambda_handler()` (line 143) - Main entry point
- `build_nested_affiliate_query()` (line 64) - Builds Elasticsearch query
- `extract_ltk_url_from_source()` (line 98) - Extracts LTK URL from influencer data
- `post_with_retry()` (line 57) - Sends request with retry logic

**Output**: Sends HTTP POST to LTK Validation Lambda for each influencer

---

### **Step 3: LTK Validation Lambda**

**File**: `lambda_functions/ltk_validation_handler.py`

**What it does**:
1. **Receives request** (line 11-15)
   - Extracts `user_id` and `ltk_url` from request body

2. **Checks if record exists** (line 20-21)
   - Queries DynamoDB `validation_table` with `user_id`

3. **Saves or updates** (line 23-30):
   - **If record doesn't exist**: Creates new record with `user_id` and `ltk_url`
   - **If record exists**: Updates `ltk_url` and removes `ltk_fetch_trigger` flag

**DynamoDB Table**: `validation_table`
- **Primary Key**: `user_id`
- **Fields**: `ltk_url`, `ltk_fetch_trigger` (removed after update)

**Output**: Updates DynamoDB `validation_table`

**Triggers**: DynamoDB Stream (watched by Platform Validation Lambda)

---

### **Step 4: Platform Validation Lambda (DynamoDB Stream)**

**File**: `lambda_functions/platform_validation_handler.py`

**What it does**:
1. **Watches DynamoDB Stream** (line 17-18)
   - Triggered automatically when `validation_table` changes
   - Processes `INSERT` and `MODIFY` events

2. **Checks for LTK URL** (line 41-51):
   - If `ltk_url` exists AND `ltk_fetch_trigger` doesn't exist
   - Sends message to **Scraper SQS Queue**

3. **SQS Message format**:
   ```json
   {
     "user_id": "...",
     "ltk_url": "https://www.shopltk.com/explore/username"
   }
   ```

4. **Updates DynamoDB** (line 43-46):
   - Sets `ltk_fetch_trigger = 'success'` to prevent duplicate triggers

**DynamoDB Stream Configuration**:
- **File**: `cdk/ask_velvee_service/service_stack.py` (line 284-290)
- **Table**: `validation_table`
- **Starting Position**: `LATEST` (only new changes)
- **Batch Size**: 100 records

**Output**: Message sent to Scraper SQS Queue

---

### **Step 5: Scraper SQS Queue**

**File**: `cdk/ask_velvee_service/service_stack.py` (line 123-128)

**Configuration**:
- **Visibility Timeout**: 25 minutes (max 720 minutes)
- **Dead Letter Queue**: `ScraperSQS_DLQ` (after 3 failed attempts)
- **Retention**: Default (4 days for DLQ)

**Message format**:
```json
{
  "user_id": "uuid-here",
  "ltk_url": "https://www.shopltk.com/explore/username"
}
```

**Purpose**: Holds influencers waiting to be scraped

---

### **Step 6: LTK Initial Scraper Worker (ECS Fargate)**

**File**: `ltkscraper-code/ltk_initial_scraper_worker.py`

**What it does**:

#### **6.1: Polls Scraper SQS Queue** (line 424-482)
- **Function**: `poll_scraper_sqs2()`
- **Polling**: 
  - `WaitTimeSeconds=10` - Waits up to 10 seconds for messages
  - `MaxNumberOfMessages=1` - Processes one message at a time
  - `VisibilityTimeout=600` - 10 minutes to process
- **Idle handling**: Exits after 10 consecutive empty polls

#### **6.2: Extracts Username** (line 460)
- Parses `ltk_url` to get username
- Example: `https://www.shopltk.com/explore/anuwaytostyle` → `anuwaytostyle`

#### **6.3: Fetches Profile ID** (line 114-150)
- **Function**: `fetch_profile_id()`
- Calls ShopLTK API: `https://www.shopltk.com/api/v5/profile/get?username={username}`
- Extracts `profile_id` from response
- **Fallback**: Uses utility function `extract_profile_id_from_username_dp_cdn_image()`

#### **6.4: Scrapes Posts from ShopLTK API** (line 152-199)
- **Function**: `extract_data()`
- **API Endpoint**: `https://api-gateway.rewardstyle.com/api/ltk/v2/search/shop`
- **Pagination**:
  - `posts_per_page = 36`
  - `MAX_PAGES = 14` (max 504 posts)
  - **Current limit**: Only processes **10 posts** (line 463)
- **Request payload**:
  ```json
  {
    "query": "",
    "ranking": "recent",
    "profile_id": "...",
    "page": 0,
    "limit": 36,
    "filters": [],
    "analytics": ["version:3.508.0-CS-216.1", "platform:web"]
  }
  ```

#### **6.5: Deduplication Check** (line 309-377)
- **Function**: `get_existing_post_ids()`
- **How it works**:
  1. Collects all post IDs from API response
  2. Batches into groups of 100
  3. Queries DynamoDB `DataModellingTable` using `batch_get_item()`
  4. Returns set of existing post IDs
  5. Skips posts that already exist
- **Code**:
  ```python
  existing_post_ids = self.get_existing_post_ids(post_ids)
  
  for pid in new_post_ids:
      if pid in existing_post_ids:
          logger.info(f'🔁 Post {pid} already exists. Skipping.')
          continue
  ```

#### **6.6: Creates Post Data** (line 382-406)
- For each **new** post, creates post object:
  ```python
  {
    'post_id': pid,
    'look_id': [uuid],
    'caption': hit.get('caption', ''),
    'image_url': hit.get('image_link', ''),
    'video_url': hit.get('video_link', ''),
    'media_type': hit.get('media_type', 'picture'),
    'date_timestamp': hit.get('date_timestamp', 0),
    'products': [],
    'post_urls': [],
    'platform': 'ltk',
    'user_id': self.user_id,
    'post_url': f'https://www.shopltk.com/explore/{username}/posts/{pid}'
  }
  ```

#### **6.7: Sends to Post Processor SQS** (line 409)
- **Function**: `send_to_sqs()`
- Sends post data as JSON to `POST_PROCESSOR_SQS_URL`
- **Retry logic**: 3 attempts with exponential backoff

#### **6.8: Deletes Message from Scraper SQS** (line 467)
- After successful processing, deletes message from queue
- Prevents reprocessing

**Runs on**: ECS Fargate (containerized service)
- **File**: `cdk/ask_velvee_service/service_stack.py` (line 520-600)

---

## 🔄 Post Ingestion Flow (Detailed)

### **Step 1: Post Processor SQS Queue**

**File**: `cdk/ask_velvee_service/service_stack.py` (line 600-610)

**Configuration**:
- **Visibility Timeout**: 240 seconds (4 minutes)
- **Message Format**: Raw post data from scraper

**Message Example**:
```json
{
  "post_id": "06958565-4025-11f0-aab2-0242ac110016",
  "caption": "Love this outfit!",
  "image_url": "https://cdn.shopltk.com/image.jpg",
  "user_id": "uuid-here",
  "platform": "ltk",
  "date_timestamp": 1234567890
}
```

---

### **Step 2: Post Processor Worker (ECS Fargate)**

**File**: `ltkscraper-code/post_processor_sqs_worker.py`

**What it does**:

#### **2.1: Polls Post Processor SQS** (line 1375-1495)
- **Function**: `poll_post_processor_sqs()`
- **Polling**:
  - `WaitTimeSeconds=10`
  - `MaxNumberOfMessages=1`
  - `VisibilityTimeout=240` (4 minutes)
- **Idle handling**: Exits after 8 consecutive empty polls

#### **2.2: Enriches Post with Media** (line 1418)
- **Function**: `LTKDataExtractor.process_post()`
- **What it does**:
  1. **Downloads images/videos** from `image_url` or `video_url`
  2. **Uploads to S3**: `ASK_VELVEE_RAG_BUCKET_SANDBOX_IA`
  3. **Uploads to Cloudflare CDN**: For fast delivery
  4. **Updates post data** with `s3_url` and `cdn_url`
  5. **Extracts affiliate links** from post page
  6. **Gets product details** from affiliate links

**File locations**:
- Download: `ltkscraper-code/post_processor_sqs_worker.py` (line 400-600)
- S3 Upload: `ltkscraper-code/utils/s3Interact.py`
- CDN Upload: `ltkscraper-code/utils/cdn_uploader.py`

#### **2.3: Enhances with Products (V2)** (line 1440)
- **Function**: `_enhance_posts_with_product_details()`
- **What it does**:
  1. Scrapes product pages from affiliate links
  2. Extracts: product name, price, description, images
  3. Resolves domain names
  4. Gets brand information
  5. Creates product objects with:
     ```python
     {
       'product_id': uuid,
       'brand_id': uuid,
       'product_affiliate_link': '...',
       'price': '$29.99',
       'description': '...',
       's3_media_url': {'cdn_url': '...', 's3_url': '...'}
     }
     ```

#### **2.4: Fallback to V3 Enhancement** (line 1462)
- **If V2 fails or no products found**:
  - **Function**: `enhance_post_with_v3_and_merge()`
  - Uses fallback product extraction methods
  - Scrapes product blocks from post page

#### **2.5: Saves to DynamoDB** (line 1476)
- **Function**: `_save_to_dynamodb()` (line 1268)
- **What it does**:
  1. **Sanitizes data**:
     - Converts floats to Decimals (DynamoDB requirement)
     - Removes empty strings
     - Removes temporary fields (`local_image_path`, `original_hit`, etc.)
  2. **Saves to `DataModellingTable`**:
     - Primary key: `id` (from `post_id`)
     - Includes: `user_id`, `caption`, `post_urls`, `products`, etc.
  3. **Saves affiliate links** to `AffiliateTable`:
     - Partition key: `influencer_id`
     - Sort key: `product_id`
     - Value: `affiliate_link`
  4. **Handles failed product uploads**:
     - Saves to `ProductUploadQueue` for retry

**DynamoDB Table**: `DataModellingTable`
- **Stream enabled**: YES
- **Triggers**: Data Modelling Lambda on INSERT/MODIFY

**File location**: `ltkscraper-code/post_processor_sqs_worker.py` (line 1268-1362)

#### **2.6: Deletes Message from Queue** (line 1487)
- After successful save, deletes message from Post Processor SQS

**Runs on**: ECS Fargate (containerized service)
- **File**: `cdk/ask_velvee_service/service_stack.py` (line 643-682)

---

### **Step 3: DynamoDB Stream Trigger**

**File**: `cdk/ask_velvee_service/service_stack.py` (line 429-437)

**Configuration**:
- **Table**: `DataModellingTable`
- **Stream View Type**: `NEW_IMAGE` (only new/updated records)
- **Starting Position**: `LATEST` (only new changes)
- **Batch Size**: 1 record at a time
- **Parallelization Factor**: 2 (processes 2 records concurrently)
- **Dead Letter Queue**: `DataModellingDLQ`

**What it does**: Automatically triggers Data Modelling Lambda when:
- New post is INSERTED into `DataModellingTable`
- Existing post is MODIFIED in `DataModellingTable`

---

### **Step 4: Data Modelling Lambda**

**File**: `service/handlers/handler_data_modelling.py`

**What it does**:

#### **4.1: Receives DynamoDB Stream Event** (line 1146-1151)
- Processes each record in the stream
- Handles `INSERT` and `MODIFY` events separately

#### **4.2: For INSERT Events** (line 1229-1750)

**Step 4.2.1: Extracts Post Data** (line 1232-1235)
- Gets post from DynamoDB stream event
- Extracts: `id`, `user_id`, `products`, `post_urls`, `caption`, etc.

**Step 4.2.2: Processes Products** (line 1240-1300)
- For each product in the post:
  1. **Calls AI (OpenAI)** to extract brand and product details:
     - **Function**: `call_llama()` (uses OpenAI GPT-3.5)
     - **Prompt**: `get_brand_product_prompt()` (line 44-145)
     - **Returns**: Brand JSON and Product JSON
  2. **Creates Brand**:
     - **Function**: `create_brand()` (line 760)
     - **API**: `POST https://endpoints.velvee.com/brand/item`
     - **Payload**: Brand name, URL, category, etc.
  3. **Creates Product**:
     - **Function**: `create_product()` (line 800)
     - **API**: `POST https://endpoints.velvee.com/product/item`
     - **Payload**: Product name, price, description, images, brand_id, etc.
  4. **Collects product IDs** for linking to looks and posts

**Step 4.2.3: Processes Images (Creates Looks)** (line 1301-1350)
- For each image URL in `post_urls`:
  1. **Calls AI (OpenAI)** to analyze image:
     - **Function**: `call_llama()` with image analysis prompt
     - **Prompt**: `get_look_prompt()` (line 148-158)
     - **Returns**: Look JSON with style description
  2. **Creates Look**:
     - **Function**: `create_look()` (line 760)
     - **API**: `POST https://endpoints.velvee.com/look/item`
     - **Payload**: 
       - `image_urls`: [image URL]
       - `product_ids`: [list of product IDs]
       - `style_description`: AI-generated description
       - `poster_id`: user_id
  3. **Collects look IDs** for linking to post

**Step 4.2.4: Creates Post** (line 1351-1400)
- **Function**: `create_post()` (line 865)
- **API**: `POST https://endpoints.velvee.com/post/item`
- **Payload**:
  ```json
  {
    "id": "post_id",
    "platform": "LTK",
    "type": "post",
    "poster_id": "user_id",
    "poster_type": "Influencer",
    "caption": "...",
    "post_urls": ["..."],
    "looks": ["look_id_1", "look_id_2"],
    "products": ["product_id_1", "product_id_2"],
    "created_date": 1234567890
  }
  ```

**Step 4.2.5: Saves Processed Data** (line 1692-1708)
- **Function**: `save_processed_data()` (line 1142)
- **Saves to**: `DataModellingProcessedTable`
- **Purpose**: Backup for MODIFY events
- **Data saved**:
  ```json
  {
    "id": "post_id",
    "processed_data": {
      "processed_brands_model": [...],
      "processed_products_model": [...],
      "processed_look_model": [...],
      "processed_post_model": [...],
      "processed_affiliate_link_model": [...]
    }
  }
  ```

#### **4.3: For MODIFY Events** (line 1152-1227)

**What it does**:
1. **Retrieves processed data** from `DataModellingProcessedTable`
2. **Re-creates all entities**:
   - Brands (line 1174-1188)
   - Products (line 1190-1204)
   - Looks (line 1206-1213)
   - Posts (line 1215-1223)
3. **Purpose**: Updates entities when post data changes

**Error Handling**: Failed creations go to DLQ (line 1180-1223)

---

## ⚠️ Error Handling & Dead Letter Queues

### **Dead Letter Queues (DLQ)**

#### 1. **ScraperSQS_DLQ**
- **Purpose**: Failed scraper messages
- **Retention**: 4 days
- **Trigger**: After 3 failed processing attempts
- **File**: `cdk/ask_velvee_service/service_stack.py` (line 122)

#### 2. **DataModellingDLQ**
- **Purpose**: Failed data modelling attempts
- **Retention**: 14 days
- **Trigger**: When Data Modelling Lambda fails
- **File**: `cdk/ask_velvee_service/service_stack.py` (line 402)

#### 3. **InstagramCloudflareRejectedDLQ**
- **Purpose**: Instagram posts with failed image uploads
- **Retention**: 14 days
- **File**: `cdk/ask_velvee_service/service_stack.py` (line 368)

### **Retry Logic**

#### **Scraper Worker**:
- **SQS Operations**: 3 retries with exponential backoff
- **File**: `ltkscraper-code/ltk_initial_scraper_worker.py` (line 92-99)

#### **Post Processor Worker**:
- **SQS Operations**: 3 retries with exponential backoff
- **S3/CDN Uploads**: Retry logic in upload functions
- **File**: `ltkscraper-code/post_processor_sqs_worker.py` (line 177-189)

#### **Daily Trigger Lambda**:
- **Elasticsearch Queries**: 3 retries with 3-second wait
- **HTTP Requests**: 3 retries with 3-second wait
- **File**: `lambda_functions/trigger_ltk_for_new_posts.py` (line 44-58)

---

## 📊 Complete Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    DAILY TRIGGER FLOW                           │
└─────────────────────────────────────────────────────────────────┘

1. EventBridge (Midnight UTC)
   └─> trigger_ltk_for_new_posts Lambda
       └─> Elasticsearch (influencer_index)
           └─> Finds influencers with LTK URLs
               └─> POST to ltk_validation_handler Lambda
                   └─> DynamoDB: validation_table
                       └─> DynamoDB Stream
                           └─> platform_validation_handler Lambda
                               └─> ScraperSQS Queue
                                   └─> LTK Initial Scraper Worker (ECS)
                                       └─> ShopLTK API
                                           └─> Deduplication Check
                                               └─> PostProcessorSQS Queue


┌─────────────────────────────────────────────────────────────────┐
│                    POST INGESTION FLOW                          │
└─────────────────────────────────────────────────────────────────┘

PostProcessorSQS Queue
  └─> Post Processor Worker (ECS)
      ├─> Downloads images/videos
      ├─> Uploads to S3 (ASK_VELVEE_RAG_BUCKET)
      ├─> Uploads to Cloudflare CDN
      ├─> Extracts affiliate links
      ├─> Enhances with products (V2/V3)
      └─> Saves to DynamoDB: DataModellingTable
          └─> DynamoDB Stream
              └─> Data Modelling Lambda
                  ├─> AI Processing (OpenAI)
                  ├─> Creates Brands (API)
                  ├─> Creates Products (API)
                  ├─> Creates Looks (API)
                  ├─> Creates Posts (API)
                  └─> Saves to DataModellingProcessedTable
```

---

## 🔑 Key Takeaways

1. **Deduplication happens at scraper level** - Only new posts go to Post Processor
2. **Data is stored in multiple places**:
   - DynamoDB: Structured data (posts, products, brands)
   - S3: Raw media files (images, videos)
   - Cloudflare CDN: Optimized images for fast delivery
   - Elasticsearch: Searchable influencer profiles
3. **Two main flows**:
   - **Daily Trigger**: Finds influencers → Scrapes posts
   - **Post Ingestion**: Enriches posts → Creates entities
4. **Error handling**: DLQs capture failed messages for manual review
5. **Retry logic**: All critical operations have retry mechanisms

---

## 📁 File Reference Summary

### **Daily Trigger Flow**:
- EventBridge Rule: `cdk/ask_velvee_service/service_stack.py` (line 740)
- Trigger Lambda: `lambda_functions/trigger_ltk_for_new_posts.py`
- Validation Lambda: `lambda_functions/ltk_validation_handler.py`
- Platform Handler: `lambda_functions/platform_validation_handler.py`
- Scraper Worker: `ltkscraper-code/ltk_initial_scraper_worker.py`

### **Post Ingestion Flow**:
- Post Processor Worker: `ltkscraper-code/post_processor_sqs_worker.py`
- Data Modelling Lambda: `service/handlers/handler_data_modelling.py`
- CDK Stack: `cdk/ask_velvee_service/service_stack.py` (lines 402-443)

### **Storage**:
- S3 Upload: `ltkscraper-code/utils/s3Interact.py`
- CDN Upload: `ltkscraper-code/utils/cdn_uploader.py`
- DynamoDB Tables: `cdk/ask_velvee_service/service_stack.py` (lines 150, 228, 237, 495, 505, 1498)

