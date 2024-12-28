CREATE TABLE xrpl_rich_list (
    id BIGSERIAL PRIMARY KEY,
    rank INTEGER NOT NULL,
    address VARCHAR(35) NOT NULL,  -- XRPLアドレスは34文字
    label TEXT,                    -- アカウントのラベル（Unknown可）
    balance_xrp DECIMAL(20, 6),    -- XRPの残高
    escrow_xrp DECIMAL(20, 6),     -- エスクローのXRP残高
    percentage DECIMAL(6, 3),      -- 全体に対する保有割合（%）
    exists BOOLEAN NOT NULL,       -- アカウントの存在状態
    snapshot_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- インデックス作成
    CONSTRAINT xrpl_rich_list_unique_snapshot UNIQUE (address, snapshot_date)
);

-- domainカラムを追加
ALTER TABLE xrpl_rich_list ADD COLUMN domain TEXT;

-- 効率的な検索のためのインデックス
CREATE INDEX idx_xrpl_rich_list_snapshot_date ON xrpl_rich_list(snapshot_date);
CREATE INDEX idx_xrpl_rich_list_address ON xrpl_rich_list(address);
CREATE INDEX idx_xrpl_rich_list_rank ON xrpl_rich_list(rank);
CREATE INDEX idx_xrpl_rich_list_exists ON xrpl_rich_list(exists);
CREATE INDEX idx_xrpl_rich_list_domain ON xrpl_rich_list(domain);


CREATE TABLE xrpl_rich_list_summary (
    id SERIAL PRIMARY KEY,
    grouped_label VARCHAR(255),
    count INTEGER,
    total_balance NUMERIC,
    total_escrow NUMERIC,
    total_xrp NUMERIC, -- balance + escrowの合計
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_summary_created_at ON xrpl_rich_list_summary(created_at);
CREATE INDEX idx_summary_grouped_label ON xrpl_rich_list_summary(grouped_label);

-- テーブル定義
CREATE TABLE xrpl_rich_list_changes (
    id SERIAL PRIMARY KEY,
    grouped_label VARCHAR(255),
    hours INTEGER,  -- 1, 3, 24, 168, 720
    balance_change NUMERIC,
    percentage_change NUMERIC,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス
CREATE INDEX idx_changes_label ON xrpl_rich_list_changes(grouped_label);
CREATE INDEX idx_changes_calculated_at ON xrpl_rich_list_changes(calculated_at);
CREATE INDEX idx_changes_hours ON xrpl_rich_list_changes(hours);

-- テーブル定義
CREATE TABLE xrpl_rich_list_available_changes (
    id SERIAL PRIMARY KEY,
    grouped_label VARCHAR(255),
    hours INTEGER,  -- 1, 3, 24, 168, 720
    balance_change NUMERIC,
    percentage_change NUMERIC,
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- インデックス
CREATE INDEX idx_available_changes_label ON xrpl_rich_list_available_changes(grouped_label);
CREATE INDEX idx_available_changes_calculated_at ON xrpl_rich_list_available_changes(calculated_at);
CREATE INDEX idx_available_changes_hours ON xrpl_rich_list_available_changes(hours);

-- XRPLラベルのカテゴリ分類テーブル
CREATE TABLE xrpl_rich_list_categories (
    id SERIAL PRIMARY KEY,
    grouped_label VARCHAR(255) NOT NULL,
    category VARCHAR(50) NOT NULL,  -- Major Contributor, Exchange, etc.
    country VARCHAR(50) NOT NULL DEFAULT 'Unknown',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 一意制約
    CONSTRAINT unique_grouped_label UNIQUE (grouped_label),
    
    -- カテゴリの制約
    CONSTRAINT valid_category CHECK (
        category IN (
            'Major Contributor',
            'Exchange',
            'Casino/Gambling',
            'Payment Service',
            'DeFi Protocol',
            'Trading Service',
            'NFT/Gaming',
            'Custody/Institution',
            'Individual',
            'Other'
        )
    )
);

-- インデックス
CREATE INDEX idx_categories_grouped_label ON xrpl_rich_list_categories(grouped_label);
CREATE INDEX idx_categories_category ON xrpl_rich_list_categories(category);
CREATE INDEX idx_categories_country ON xrpl_rich_list_categories(country);

-- 更新日時を自動的に設定するトリガー関数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 更新日時の自動更新トリガー
CREATE TRIGGER update_categories_updated_at
    BEFORE UPDATE ON xrpl_rich_list_categories
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- カテゴリごとの合計変化を追跡するテーブル
CREATE TABLE xrpl_rich_list_category_changes (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    hours INTEGER NOT NULL,  -- 1, 3, 24, 168, 720
    count INTEGER,
    total_balance NUMERIC NOT NULL,
    total_escrow NUMERIC NOT NULL,
    total_xrp NUMERIC NOT NULL,
    balance_change NUMERIC,
    percentage_change NUMERIC,
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- カテゴリの制約（xrpl_rich_list_categoriesと同じ制約を維持）
    CONSTRAINT valid_category CHECK (
        category IN (
            'Major Contributor',
            'Exchange',
            'Casino/Gambling',
            'Payment Service',
            'DeFi Protocol',
            'Trading Service',
            'NFT/Gaming',
            'Custody/Institution',
            'Individual',
            'Other'
        )
    )
);

-- 効率的な検索のためのインデックス
CREATE INDEX idx_category_changes_category ON xrpl_rich_list_category_changes(category);
CREATE INDEX idx_category_changes_hours ON xrpl_rich_list_category_changes(hours);
CREATE INDEX idx_category_changes_calculated_at ON xrpl_rich_list_category_changes(calculated_at);


-- 国別の合計変化を追跡するテーブル
CREATE TABLE xrpl_rich_list_country_changes (
    id SERIAL PRIMARY KEY,
    country VARCHAR(50) NOT NULL,
    hours INTEGER NOT NULL,  -- 1, 3, 24, 168, 720
    count INTEGER,
    total_balance NUMERIC NOT NULL,
    total_escrow NUMERIC NOT NULL,
    total_xrp NUMERIC NOT NULL,
    balance_change NUMERIC,
    percentage_change NUMERIC,
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Unknown をデフォルトとして許可
    CONSTRAINT valid_country CHECK (country <> '')
);

-- 効率的な検索のためのインデックス
CREATE INDEX idx_country_changes_country ON xrpl_rich_list_country_changes(country);
CREATE INDEX idx_country_changes_hours ON xrpl_rich_list_country_changes(hours);
CREATE INDEX idx_country_changes_calculated_at ON xrpl_rich_list_country_changes(calculated_at);

-- エスクロー抜きの時系列データを保存するテーブル
CREATE TABLE xrpl_rich_list_available_hourly (
    id SERIAL PRIMARY KEY,
    grouped_label VARCHAR(255) NOT NULL,
    count INTEGER,
    total_balance NUMERIC NOT NULL,
    total_escrow NUMERIC NOT NULL,
    total_xrp NUMERIC NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- 同じ時間のデータが重複しないように
    CONSTRAINT unique_available_hour UNIQUE (grouped_label, created_at)
);

-- インデックスの作成
CREATE INDEX idx_available_hourly_timestamp ON xrpl_rich_list_available_hourly(created_at);
CREATE INDEX idx_available_hourly_label ON xrpl_rich_list_available_hourly(grouped_label);

-- カテゴリごとの時系列データを保存するテーブル
CREATE TABLE xrpl_rich_list_category_hourly (
    id SERIAL PRIMARY KEY,
    grouped_label VARCHAR(50) NOT NULL,
    count INTEGER,
    total_balance NUMERIC NOT NULL,
    total_escrow NUMERIC NOT NULL,
    total_xrp NUMERIC NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- カテゴリの制約（既存のテーブルと同じ）
    CONSTRAINT valid_category_hourly CHECK (
        grouped_label IN (
            'Major Contributor',
            'Exchange',
            'Casino/Gambling',
            'Payment Service',
            'DeFi Protocol',
            'Trading Service',
            'NFT/Gaming',
            'Custody/Institution',
            'Individual',
            'Other'
        )
    ),
    
    -- 同じ時間のデータが重複しないように
    CONSTRAINT unique_category_hour UNIQUE (grouped_label, created_at)
);

-- インデックスの作成
CREATE INDEX idx_category_hourly_timestamp ON xrpl_rich_list_category_hourly(created_at);
CREATE INDEX idx_category_hourly_category ON xrpl_rich_list_category_hourly(grouped_label);

-- 国ごとの時系列データを保存するテーブル
CREATE TABLE xrpl_rich_list_country_hourly (
    id SERIAL PRIMARY KEY,
    grouped_label VARCHAR(50) NOT NULL,
    count INTEGER,
    total_balance NUMERIC NOT NULL,
    total_escrow NUMERIC NOT NULL,
    total_xrp NUMERIC NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- 国名の制約
    CONSTRAINT valid_country_hourly CHECK (grouped_label <> ''),
    
    -- 同じ時間のデータが重複しないように
    CONSTRAINT unique_country_hour UNIQUE (grouped_label, created_at)
);

-- インデックスの作成
CREATE INDEX idx_country_hourly_timestamp ON xrpl_rich_list_country_hourly(created_at);
CREATE INDEX idx_country_hourly_country ON xrpl_rich_list_country_hourly(grouped_label);


-- ビュー定義
CREATE VIEW xrpl_rich_list_summary_with_changes AS
WITH latest_summary AS (
    SELECT *
    FROM xrpl_rich_list_summary
    WHERE created_at = (
        SELECT created_at
        FROM xrpl_rich_list_summary
        ORDER BY created_at DESC
        LIMIT 1
    )
)
SELECT 
    s.*,
    h1.balance_change as change_1h,
    h1.percentage_change as percentage_1h,
    h3.balance_change as change_3h,
    h3.percentage_change as percentage_3h,
    h24.balance_change as change_24h,
    h24.percentage_change as percentage_24h,
    h168.balance_change as change_168h,
    h168.percentage_change as percentage_168h,
    h720.balance_change as change_720h,
    h720.percentage_change as percentage_720h
FROM latest_summary s
LEFT JOIN xrpl_rich_list_changes h1 
    ON s.grouped_label = h1.grouped_label 
    AND h1.hours = 1
LEFT JOIN xrpl_rich_list_changes h3
    ON s.grouped_label = h3.grouped_label 
    AND h3.hours = 3
LEFT JOIN xrpl_rich_list_changes h24
    ON s.grouped_label = h24.grouped_label 
    AND h24.hours = 24
LEFT JOIN xrpl_rich_list_changes h168
    ON s.grouped_label = h168.grouped_label 
    AND h168.hours = 168
LEFT JOIN xrpl_rich_list_changes h720
    ON s.grouped_label = h720.grouped_label 
    AND h720.hours = 720
ORDER BY s.total_xrp DESC;

-- XRPのビュー
CREATE VIEW xrpl_rich_list_summary_with_total_changes AS
WITH latest_summary AS (
    SELECT *
    FROM xrpl_rich_list_summary
    WHERE created_at = (
        SELECT created_at
        FROM xrpl_rich_list_summary
        ORDER BY created_at DESC
        LIMIT 1
    )
)
SELECT 
    s.*,
    s.total_xrp as show_total_xrp,
    c.category as entity_category,
    c.country as entity_country,
    h1.balance_change as change_1h,
    h1.percentage_change as percentage_1h,
    h3.balance_change as change_3h,
    h3.percentage_change as percentage_3h,
    h24.balance_change as change_24h,
    h24.percentage_change as percentage_24h,
    h168.balance_change as change_168h,
    h168.percentage_change as percentage_168h,
    h720.balance_change as change_720h,
    h720.percentage_change as percentage_720h
FROM latest_summary s
LEFT JOIN xrpl_rich_list_categories c
    ON s.grouped_label = c.grouped_label
LEFT JOIN xrpl_rich_list_changes h1 
    ON s.grouped_label = h1.grouped_label 
    AND h1.hours = 1
LEFT JOIN xrpl_rich_list_changes h3
    ON s.grouped_label = h3.grouped_label 
    AND h3.hours = 3
LEFT JOIN xrpl_rich_list_changes h24
    ON s.grouped_label = h24.grouped_label 
    AND h24.hours = 24
LEFT JOIN xrpl_rich_list_changes h168
    ON s.grouped_label = h168.grouped_label 
    AND h168.hours = 168
LEFT JOIN xrpl_rich_list_changes h720
    ON s.grouped_label = h720.grouped_label 
    AND h720.hours = 720
ORDER BY s.total_xrp DESC;

-- Available XRPのビュー
CREATE VIEW xrpl_rich_list_summary_with_available_changes AS
WITH latest_summary AS (
   SELECT *
   FROM xrpl_rich_list_summary
   WHERE created_at = (
       SELECT created_at
       FROM xrpl_rich_list_summary
       ORDER BY created_at DESC
       LIMIT 1
   )
)
SELECT 
   s.*,
   s.total_balance as show_total_xrp,
   c.category as entity_category,
   c.country as entity_country,
   h1.balance_change as change_1h,
   h1.percentage_change as percentage_1h,
   h3.balance_change as change_3h,
   h3.percentage_change as percentage_3h,
   h24.balance_change as change_24h,
   h24.percentage_change as percentage_24h,
   h168.balance_change as change_168h,
   h168.percentage_change as percentage_168h,
   h720.balance_change as change_720h,
   h720.percentage_change as percentage_720h
FROM latest_summary s
LEFT JOIN xrpl_rich_list_categories c
   ON s.grouped_label = c.grouped_label
LEFT JOIN xrpl_rich_list_available_changes h1 
   ON s.grouped_label = h1.grouped_label 
   AND h1.hours = 1
LEFT JOIN xrpl_rich_list_available_changes h3
   ON s.grouped_label = h3.grouped_label 
   AND h3.hours = 3
LEFT JOIN xrpl_rich_list_available_changes h24
   ON s.grouped_label = h24.grouped_label 
   AND h24.hours = 24
LEFT JOIN xrpl_rich_list_available_changes h168
   ON s.grouped_label = h168.grouped_label 
   AND h168.hours = 168
LEFT JOIN xrpl_rich_list_available_changes h720
   ON s.grouped_label = h720.grouped_label 
   AND h720.hours = 720
ORDER BY s.total_balance DESC;


-- カテゴリ別のサマリービューを作成
CREATE OR REPLACE VIEW xrpl_rich_list_category_summary_with_changes AS
WITH latest_changes AS (
    SELECT DISTINCT
        category,
        MAX(calculated_at) as latest_calculated_at
    FROM xrpl_rich_list_category_changes
    GROUP BY category
),
category_data AS (
    SELECT DISTINCT
        c.category as grouped_label,
        c.count as count,
        c.total_balance,
        c.total_escrow,
        c.total_xrp,
        c.calculated_at as created_at,
        c.total_xrp as show_total_xrp,
        c.category as entity_category,
        '-' as entity_country,
        h1.balance_change as change_1h,
        h1.percentage_change as percentage_1h,
        h3.balance_change as change_3h,
        h3.percentage_change as percentage_3h,
        h24.balance_change as change_24h,
        h24.percentage_change as percentage_24h,
        h168.balance_change as change_168h,
        h168.percentage_change as percentage_168h,
        h720.balance_change as change_720h,
        h720.percentage_change as percentage_720h
    FROM latest_changes lc
    JOIN xrpl_rich_list_category_changes c 
        ON c.category = lc.category 
        AND c.calculated_at = lc.latest_calculated_at
    LEFT JOIN xrpl_rich_list_category_changes h1 
        ON c.category = h1.category 
        AND h1.hours = 1
        AND h1.calculated_at = c.calculated_at
    LEFT JOIN xrpl_rich_list_category_changes h3
        ON c.category = h3.category 
        AND h3.hours = 3
        AND h3.calculated_at = c.calculated_at
    LEFT JOIN xrpl_rich_list_category_changes h24
        ON c.category = h24.category 
        AND h24.hours = 24
        AND h24.calculated_at = c.calculated_at
    LEFT JOIN xrpl_rich_list_category_changes h168
        ON c.category = h168.category 
        AND h168.hours = 168
        AND h168.calculated_at = c.calculated_at
    LEFT JOIN xrpl_rich_list_category_changes h720
        ON c.category = h720.category 
        AND h720.hours = 720
        AND h720.calculated_at = c.calculated_at
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY total_xrp DESC) as id,
    *
FROM category_data
ORDER BY total_xrp DESC;


-- 国別の最新データを表示するビュー
CREATE VIEW xrpl_rich_list_country_summary_with_changes AS
WITH latest_changes AS (
    SELECT 
        country,
        MAX(calculated_at) as latest_calculated_at
    FROM xrpl_rich_list_country_changes
    GROUP BY country
),
country_data AS (
    SELECT DISTINCT
        c.country as grouped_label,
        c.count as count,
        c.total_balance,
        c.total_escrow,
        c.total_xrp,
        c.calculated_at as created_at,
        c.total_xrp as show_total_xrp,
        '-' as entity_category,
        c.country as entity_country,
        h1.balance_change as change_1h,
        h1.percentage_change as percentage_1h,
        h3.balance_change as change_3h,
        h3.percentage_change as percentage_3h,
        h24.balance_change as change_24h,
        h24.percentage_change as percentage_24h,
        h168.balance_change as change_168h,
        h168.percentage_change as percentage_168h,
        h720.balance_change as change_720h,
        h720.percentage_change as percentage_720h
    FROM latest_changes lc
    JOIN xrpl_rich_list_country_changes c 
        ON c.country = lc.country 
        AND c.calculated_at = lc.latest_calculated_at
    LEFT JOIN xrpl_rich_list_country_changes h1 
        ON c.country = h1.country 
        AND h1.hours = 1
        AND h1.calculated_at = c.calculated_at
    LEFT JOIN xrpl_rich_list_country_changes h3
        ON c.country = h3.country 
        AND h3.hours = 3
        AND h3.calculated_at = c.calculated_at
    LEFT JOIN xrpl_rich_list_country_changes h24
        ON c.country = h24.country 
        AND h24.hours = 24
        AND h24.calculated_at = c.calculated_at
    LEFT JOIN xrpl_rich_list_country_changes h168
        ON c.country = h168.country 
        AND h168.hours = 168
        AND h168.calculated_at = c.calculated_at
    LEFT JOIN xrpl_rich_list_country_changes h720
        ON c.country = h720.country 
        AND h720.hours = 720
        AND h720.calculated_at = c.calculated_at
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY total_xrp DESC) as id,
    *
FROM country_data
ORDER BY total_xrp DESC;
