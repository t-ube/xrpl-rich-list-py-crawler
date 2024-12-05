-- サマリーテーブル更新用の関数
create or replace function update_rich_list_summary()
returns void
language plpgsql
security definer
as $$
begin
    INSERT INTO xrpl_rich_list_summary (grouped_label, count, total_balance, total_escrow, total_xrp, created_at)
    WITH latest_snapshot AS (
        SELECT snapshot_date 
        FROM xrpl_rich_list 
        ORDER BY snapshot_date DESC 
        LIMIT 1
    )
    SELECT 
        CASE 
            WHEN label LIKE 'Ripple%' THEN 'Ripple'
            WHEN label LIKE 'Coinbase%' THEN 'Coinbase'
            WHEN label LIKE 'Bitrue%' THEN 'Bitrue'
            WHEN label LIKE 'Binance%' THEN 'Binance'
            WHEN label LIKE 'WhiteBIT%' THEN 'WhiteBIT'
            WHEN label LIKE 'CoinCola%' THEN 'CoinCola'
            WHEN label LIKE '%gatehub%' THEN 'gatehub'
            WHEN label LIKE 'Crypto.com%' THEN 'Crypto.com'
            ELSE REGEXP_REPLACE(
                REGEXP_REPLACE(label, '^~', ''),
                '\s*\([0-9]+\)$', ''
            )
        END AS grouped_label,
        COUNT(*) as count,
        SUM(balance_xrp) as total_balance,
        SUM(escrow_xrp) as total_escrow,
        SUM(balance_xrp + escrow_xrp) as total_xrp,
        (SELECT snapshot_date FROM latest_snapshot) as created_at
    FROM xrpl_rich_list
    WHERE snapshot_date = (SELECT snapshot_date FROM latest_snapshot)
    GROUP BY 
        CASE 
            WHEN label LIKE 'Ripple%' THEN 'Ripple'
            WHEN label LIKE 'Coinbase%' THEN 'Coinbase'
            WHEN label LIKE 'Bitrue%' THEN 'Bitrue'
            WHEN label LIKE 'Binance%' THEN 'Binance'
            WHEN label LIKE 'WhiteBIT%' THEN 'WhiteBIT'
            WHEN label LIKE 'CoinCola%' THEN 'CoinCola'
            WHEN label LIKE '%gatehub%' THEN 'gatehub'
            WHEN label LIKE 'Crypto.com%' THEN 'Crypto.com'
            ELSE REGEXP_REPLACE(
                REGEXP_REPLACE(label, '^~', ''),
                '\s*\([0-9]+\)$', ''
            )
        END;
end;
$$;

-- 残高変更更新用の関数
CREATE OR REPLACE FUNCTION update_balance_changes()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    -- 既存のデータを削除
    DELETE FROM xrpl_rich_list_changes WHERE TRUE;
    
    -- 新しいデータを挿入
    WITH current_totals AS (
        SELECT 
            grouped_label,
            total_xrp,
            created_at
        FROM xrpl_rich_list_summary
        WHERE created_at = (
            SELECT created_at 
            FROM xrpl_rich_list_summary 
            ORDER BY created_at DESC 
            LIMIT 1
        )
    ),
    period_changes AS (
        SELECT 
            c.grouped_label,
            -- 2時間の変化
            c.total_xrp - COALESCE(h2.total_xrp, c.total_xrp) as hour_2_change,
            CASE 
                WHEN COALESCE(h2.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h2.total_xrp, c.total_xrp)) / COALESCE(h2.total_xrp, c.total_xrp) * 100)
            END as hour_2_percentage,
            -- 4時間の変化
            c.total_xrp - COALESCE(h4.total_xrp, c.total_xrp) as hour_4_change,
            CASE 
                WHEN COALESCE(h4.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h4.total_xrp, c.total_xrp)) / COALESCE(h4.total_xrp, c.total_xrp) * 100)
            END as hour_4_percentage,
            -- 6時間の変化
            c.total_xrp - COALESCE(h6.total_xrp, c.total_xrp) as hour_6_change,
            CASE 
                WHEN COALESCE(h6.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h6.total_xrp, c.total_xrp)) / COALESCE(h6.total_xrp, c.total_xrp) * 100)
            END as hour_6_percentage,
            -- 12時間の変化
            c.total_xrp - COALESCE(h12.total_xrp, c.total_xrp) as hour_12_change,
            CASE 
                WHEN COALESCE(h12.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h12.total_xrp, c.total_xrp)) / COALESCE(h12.total_xrp, c.total_xrp) * 100)
            END as hour_12_percentage,
            -- 24時間の変化
            c.total_xrp - COALESCE(h24.total_xrp, c.total_xrp) as hour_24_change,
            CASE 
                WHEN COALESCE(h24.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h24.total_xrp, c.total_xrp)) / COALESCE(h24.total_xrp, c.total_xrp) * 100)
            END as hour_24_percentage,
            -- 168時間（7日）の変化
            c.total_xrp - COALESCE(h168.total_xrp, c.total_xrp) as hour_168_change,
            CASE 
                WHEN COALESCE(h168.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h168.total_xrp, c.total_xrp)) / COALESCE(h168.total_xrp, c.total_xrp) * 100)
            END as hour_168_percentage,
            -- 720時間（30日）の変化
            c.total_xrp - COALESCE(h720.total_xrp, c.total_xrp) as hour_720_change,
            CASE 
                WHEN COALESCE(h720.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h720.total_xrp, c.total_xrp)) / COALESCE(h720.total_xrp, c.total_xrp) * 100)
            END as hour_720_percentage
        FROM current_totals c
        -- 2時間前のデータ
        LEFT JOIN xrpl_rich_list_summary h2 ON 
            c.grouped_label = h2.grouped_label AND 
            h2.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '2 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
        -- 4時間前のデータ
        LEFT JOIN xrpl_rich_list_summary h4 ON 
            c.grouped_label = h4.grouped_label AND 
            h4.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '4 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
        -- 6時間前のデータ
        LEFT JOIN xrpl_rich_list_summary h6 ON 
            c.grouped_label = h6.grouped_label AND 
            h6.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '6 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
        -- 12時間前のデータ
        LEFT JOIN xrpl_rich_list_summary h12 ON 
            c.grouped_label = h12.grouped_label AND 
            h12.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '12 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
        -- 24時間前のデータ
        LEFT JOIN xrpl_rich_list_summary h24 ON 
            c.grouped_label = h24.grouped_label AND 
            h24.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '24 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
        -- 168時間（7日）前のデータ
        LEFT JOIN xrpl_rich_list_summary h168 ON 
            c.grouped_label = h168.grouped_label AND 
            h168.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '168 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
        -- 720時間（30日）前のデータ
        LEFT JOIN xrpl_rich_list_summary h720 ON 
            c.grouped_label = h720.grouped_label AND 
            h720.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '720 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
    )
    INSERT INTO xrpl_rich_list_changes 
        (grouped_label, hours, balance_change, percentage_change, calculated_at)
    SELECT 
        grouped_label,
        2,
        hour_2_change,
        hour_2_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        grouped_label,
        4,
        hour_4_change,
        hour_4_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        grouped_label,
        6,
        hour_6_change,
        hour_6_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        grouped_label,
        12,
        hour_12_change,
        hour_12_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        grouped_label,
        24,
        hour_24_change,
        hour_24_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        grouped_label,
        168,
        hour_168_change,
        hour_168_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        grouped_label,
        720,
        hour_720_change,
        hour_720_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes;
END;
$$;

-- データクリーンアップ用の関数
create or replace function cleanup_old_rich_list_data()
returns void
language plpgsql
security definer
as $$
begin
    -- 古いデータの削除
    DELETE FROM xrpl_rich_list
    WHERE snapshot_date < CURRENT_TIMESTAMP - INTERVAL '2 days';
    
    DELETE FROM xrpl_rich_list_summary
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '370 days';
end;
$$;