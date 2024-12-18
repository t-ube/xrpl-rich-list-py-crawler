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

-- 効率的な検索のためのインデックス
CREATE INDEX idx_xrpl_rich_list_snapshot_date ON xrpl_rich_list(snapshot_date);
CREATE INDEX idx_xrpl_rich_list_address ON xrpl_rich_list(address);
CREATE INDEX idx_xrpl_rich_list_rank ON xrpl_rich_list(rank);
CREATE INDEX idx_xrpl_rich_list_exists ON xrpl_rich_list(exists);


CREATE TABLE xrpl_rich_list_summary (
    id SERIAL PRIMARY KEY,
    grouped_label VARCHAR(255),
    count INTEGER,
    total_balance NUMERIC,
    total_escrow NUMERIC,
    total_xrp NUMERIC, -- balance + escrowの合計
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス
CREATE INDEX idx_available_changes_label ON xrpl_rich_list_available_changes(grouped_label);
CREATE INDEX idx_available_changes_calculated_at ON xrpl_rich_list_available_changes(calculated_at);
CREATE INDEX idx_available_changes_hours ON xrpl_rich_list_available_changes(hours);

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
