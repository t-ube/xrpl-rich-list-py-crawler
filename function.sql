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
create or replace function update_balance_changes()
returns void
language plpgsql
security definer
as $$
begin
    -- 既存のデータを削除
    DELETE FROM xrpl_rich_list_changes;
    
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
            1 as period_days,
            c.total_xrp - COALESCE(d1.total_xrp, c.total_xrp) as day_change,
            CASE 
                WHEN COALESCE(d1.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(d1.total_xrp, c.total_xrp)) / COALESCE(d1.total_xrp, c.total_xrp) * 100)
            END as day_percentage,
            c.total_xrp - COALESCE(d7.total_xrp, c.total_xrp) as week_change,
            CASE 
                WHEN COALESCE(d7.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(d7.total_xrp, c.total_xrp)) / COALESCE(d7.total_xrp, c.total_xrp) * 100)
            END as week_percentage,
            c.total_xrp - COALESCE(d30.total_xrp, c.total_xrp) as month_change,
            CASE 
                WHEN COALESCE(d30.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(d30.total_xrp, c.total_xrp)) / COALESCE(d30.total_xrp, c.total_xrp) * 100)
            END as month_percentage
        FROM current_totals c
        LEFT JOIN xrpl_rich_list_summary d1 ON 
            c.grouped_label = d1.grouped_label AND 
            d1.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at < c.created_at
                ORDER BY created_at DESC 
                LIMIT 1
            )
        LEFT JOIN xrpl_rich_list_summary d7 ON 
            c.grouped_label = d7.grouped_label AND 
            d7.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '7 days'
                ORDER BY created_at DESC 
                LIMIT 1
            )
        LEFT JOIN xrpl_rich_list_summary d30 ON 
            c.grouped_label = d30.grouped_label AND 
            d30.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '30 days'
                ORDER BY created_at DESC 
                LIMIT 1
            )
    )
    INSERT INTO xrpl_rich_list_changes 
        (grouped_label, period_days, balance_change, percentage_change, calculated_at)
    SELECT 
        grouped_label,
        1,
        day_change,
        day_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        grouped_label,
        7,
        week_change,
        week_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        grouped_label,
        30,
        month_change,
        month_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes;
end;
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
    WHERE snapshot_date < CURRENT_TIMESTAMP - INTERVAL '10 days';
    
    DELETE FROM xrpl_rich_list_summary
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '370 days';
end;
$$;