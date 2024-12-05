CREATE TABLE xrpl_rich_list (
    id BIGSERIAL PRIMARY KEY,
    rank INTEGER NOT NULL,
    address VARCHAR(35) NOT NULL,  -- XRPLアドレスは34文字
    label TEXT,                    -- アカウントのラベル（Unknown可）
    balance_xrp DECIMAL(20, 6),    -- XRPの残高
    escrow_xrp DECIMAL(20, 6),     -- エスクローのXRP残高
    percentage DECIMAL(6, 3),      -- 全体に対する保有割合（%）
    snapshot_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- インデックス作成
    CONSTRAINT xrpl_rich_list_unique_snapshot UNIQUE (address, snapshot_date)
);

-- 効率的な検索のためのインデックス
CREATE INDEX idx_xrpl_rich_list_snapshot_date ON xrpl_rich_list(snapshot_date);
CREATE INDEX idx_xrpl_rich_list_address ON xrpl_rich_list(address);
CREATE INDEX idx_xrpl_rich_list_rank ON xrpl_rich_list(rank);


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
    hours INTEGER,  -- 2, 4, 6, 12, 24, 168, 720
    balance_change NUMERIC,
    percentage_change NUMERIC,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス
CREATE INDEX idx_changes_label ON xrpl_rich_list_changes(grouped_label);
CREATE INDEX idx_changes_calculated_at ON xrpl_rich_list_changes(calculated_at);
CREATE INDEX idx_changes_hours ON xrpl_rich_list_changes(hours);

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
    h2.balance_change as change_2h,
    h2.percentage_change as percentage_2h,
    h4.balance_change as change_4h,
    h4.percentage_change as percentage_4h,
    h6.balance_change as change_6h,
    h6.percentage_change as percentage_6h,
    h12.balance_change as change_12h,
    h12.percentage_change as percentage_12h,
    h24.balance_change as change_24h,
    h24.percentage_change as percentage_24h,
    h168.balance_change as change_168h,
    h168.percentage_change as percentage_168h,
    h720.balance_change as change_720h,
    h720.percentage_change as percentage_720h
FROM latest_summary s
LEFT JOIN xrpl_rich_list_changes h2 
    ON s.grouped_label = h2.grouped_label 
    AND h2.hours = 2
LEFT JOIN xrpl_rich_list_changes h4
    ON s.grouped_label = h4.grouped_label 
    AND h4.hours = 4
LEFT JOIN xrpl_rich_list_changes h6
    ON s.grouped_label = h6.grouped_label 
    AND h6.hours = 6
LEFT JOIN xrpl_rich_list_changes h12
    ON s.grouped_label = h12.grouped_label 
    AND h12.hours = 12
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
