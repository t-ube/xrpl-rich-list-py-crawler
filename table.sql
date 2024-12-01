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

CREATE TABLE xrpl_rich_list_changes (
    id SERIAL PRIMARY KEY,
    grouped_label VARCHAR(255),
    period_days INTEGER,  -- 1, 7, 30, etc.
    balance_change NUMERIC,
    percentage_change NUMERIC,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_changes_label ON xrpl_rich_list_changes(grouped_label);
CREATE INDEX idx_changes_calculated_at ON xrpl_rich_list_changes(calculated_at);

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
    d1.balance_change as change_1d,
    d1.percentage_change as percentage_1d,
    d7.balance_change as change_7d,
    d7.percentage_change as percentage_7d,
    d30.balance_change as change_30d,
    d30.percentage_change as percentage_30d
FROM latest_summary s
LEFT JOIN xrpl_rich_list_changes d1 
    ON s.grouped_label = d1.grouped_label 
    AND d1.period_days = 1
LEFT JOIN xrpl_rich_list_changes d7
    ON s.grouped_label = d7.grouped_label 
    AND d7.period_days = 7
LEFT JOIN xrpl_rich_list_changes d30
    ON s.grouped_label = d30.grouped_label 
    AND d30.period_days = 30
ORDER BY s.total_xrp DESC;
