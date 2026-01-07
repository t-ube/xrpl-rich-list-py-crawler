-- サマリーテーブル更新用の関数
create or replace function update_rich_list_summary()
returns void
language plpgsql
security definer
SET statement_timeout = '60s'
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
            WHEN label LIKE 'bithomp%' THEN 'Bithomp'
            WHEN label LIKE 'Bithomp%' THEN 'Bithomp'
            WHEN label LIKE 'Bithumb%' THEN 'Bithumb'
            WHEN label LIKE 'Binance%' THEN 'Binance'
            WHEN label LIKE 'WhiteBIT%' THEN 'WhiteBIT'
            WHEN label LIKE 'CoinCola%' THEN 'CoinCola'
            WHEN label LIKE 'CoinSwitch%' THEN 'CoinSwitch'
            WHEN label LIKE '%gatehub%' THEN 'gatehub'
            WHEN label LIKE 'GateHub%' THEN 'gatehub'
            WHEN label LIKE 'Crypto.com%' THEN 'Crypto.com'
            WHEN label LIKE 'CROSSMARK%' THEN 'CROSSMARK'
            WHEN label LIKE 'digifin%' THEN 'Digifin'
            WHEN label LIKE 'eolas%' THEN 'eolas'
            WHEN label LIKE 'eToro%' THEN 'eToro'
            WHEN label LIKE 'Evernode Labs%' THEN 'Evernode Labs Ltd'
            WHEN label LIKE 'Evernode%' THEN 'Evernode'
            WHEN label LIKE 'FTX %' THEN 'FTX'
            WHEN label LIKE 'Hotbit%' THEN 'Hotbit'
            WHEN label LIKE 'Huobi%' THEN 'Huobi'
            WHEN label LIKE 'Northern VoIP%' THEN 'Northern VoIP'
            WHEN label LIKE 'SBI VC%' THEN 'SBI VC Trade'
            WHEN label LIKE 'Sonar Muse%' THEN 'Sonar Muse'
            WHEN label LIKE 'tequ%' THEN 'tequ'
            WHEN label LIKE 'Vagabond%' THEN 'Vagabond'
            WHEN label LIKE 'XUMM%' THEN 'XUMM'
            ELSE REGEXP_REPLACE(
                REGEXP_REPLACE(label, '^~', ''),
                '\s*\([^)]*\)$', ''
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
            WHEN label LIKE 'bithomp%' THEN 'Bithomp'
            WHEN label LIKE 'Bithomp%' THEN 'Bithomp'
            WHEN label LIKE 'Bithumb%' THEN 'Bithumb'
            WHEN label LIKE 'Binance%' THEN 'Binance'
            WHEN label LIKE 'WhiteBIT%' THEN 'WhiteBIT'
            WHEN label LIKE 'CoinCola%' THEN 'CoinCola'
            WHEN label LIKE 'CoinSwitch%' THEN 'CoinSwitch'
            WHEN label LIKE '%gatehub%' THEN 'gatehub'
            WHEN label LIKE 'GateHub%' THEN 'gatehub'
            WHEN label LIKE 'Crypto.com%' THEN 'Crypto.com'
            WHEN label LIKE 'CROSSMARK%' THEN 'CROSSMARK'
            WHEN label LIKE 'digifin%' THEN 'Digifin'
            WHEN label LIKE 'eolas%' THEN 'eolas'
            WHEN label LIKE 'eToro%' THEN 'eToro'
            WHEN label LIKE 'Evernode Labs%' THEN 'Evernode Labs Ltd'
            WHEN label LIKE 'Evernode%' THEN 'Evernode'
            WHEN label LIKE 'FTX %' THEN 'FTX'
            WHEN label LIKE 'Hotbit%' THEN 'Hotbit'
            WHEN label LIKE 'Huobi%' THEN 'Huobi'
            WHEN label LIKE 'Northern VoIP%' THEN 'Northern VoIP'
            WHEN label LIKE 'SBI VC%' THEN 'SBI VC Trade'
            WHEN label LIKE 'Sonar Muse%' THEN 'Sonar Muse'
            WHEN label LIKE 'tequ%' THEN 'tequ'
            WHEN label LIKE 'Vagabond%' THEN 'Vagabond'
            WHEN label LIKE 'XUMM%' THEN 'XUMM'
            ELSE REGEXP_REPLACE(
                REGEXP_REPLACE(label, '^~', ''),
                '\s*\([^)]*\)$', ''
            )
        END;
end;
$$;

-- 残高変更更新用の関数
CREATE OR REPLACE FUNCTION update_balance_changes()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout = '60s'
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
            -- 1時間の変化
            c.total_xrp - COALESCE(h1.total_xrp, c.total_xrp) as hour_1_change,
            CASE 
                WHEN COALESCE(h1.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h1.total_xrp, c.total_xrp)) / COALESCE(h1.total_xrp, c.total_xrp) * 100)
            END as hour_1_percentage,
            -- 3時間の変化
            c.total_xrp - COALESCE(h3.total_xrp, c.total_xrp) as hour_3_change,
            CASE 
                WHEN COALESCE(h3.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h3.total_xrp, c.total_xrp)) / COALESCE(h3.total_xrp, c.total_xrp) * 100)
            END as hour_3_percentage,
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
        -- 1時間前のデータ
        LEFT JOIN xrpl_rich_list_summary h1 ON 
            c.grouped_label = h1.grouped_label AND 
            h1.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at > c.created_at - INTERVAL '70 minutes'
                AND created_at <= c.created_at - INTERVAL '45 minutes'
                ORDER BY created_at DESC 
                LIMIT 1
            )
        -- 3時間前のデータ
        LEFT JOIN xrpl_rich_list_summary h3 ON 
            c.grouped_label = h3.grouped_label AND 
            h3.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '3 hours'
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
        1,
        hour_1_change,
        hour_1_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        grouped_label,
        3,
        hour_3_change,
        hour_3_percentage,
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


CREATE OR REPLACE FUNCTION update_available_changes()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout = '60s'
AS $$
BEGIN
    -- 既存のデータを削除
    DELETE FROM xrpl_rich_list_available_changes WHERE TRUE;
    
    -- 新しいデータを挿入
    WITH current_totals AS (
        SELECT 
            grouped_label,
            total_balance,
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
            -- 1時間の変化
            c.total_balance - COALESCE(h1.total_balance, c.total_balance) as hour_1_change,
            CASE 
                WHEN COALESCE(h1.total_balance, c.total_balance) = 0 THEN 0
                ELSE ((c.total_balance - COALESCE(h1.total_balance, c.total_balance)) / COALESCE(h1.total_balance, c.total_balance) * 100)
            END as hour_1_percentage,
            -- 3時間の変化
            c.total_balance - COALESCE(h3.total_balance, c.total_balance) as hour_3_change,
            CASE 
                WHEN COALESCE(h3.total_balance, c.total_balance) = 0 THEN 0
                ELSE ((c.total_balance - COALESCE(h3.total_balance, c.total_balance)) / COALESCE(h3.total_balance, c.total_balance) * 100)
            END as hour_3_percentage,
            -- 24時間の変化
            c.total_balance - COALESCE(h24.total_balance, c.total_balance) as hour_24_change,
            CASE 
                WHEN COALESCE(h24.total_balance, c.total_balance) = 0 THEN 0
                ELSE ((c.total_balance - COALESCE(h24.total_balance, c.total_balance)) / COALESCE(h24.total_balance, c.total_balance) * 100)
            END as hour_24_percentage,
            -- 168時間（7日）の変化
            c.total_balance - COALESCE(h168.total_balance, c.total_balance) as hour_168_change,
            CASE 
                WHEN COALESCE(h168.total_balance, c.total_balance) = 0 THEN 0
                ELSE ((c.total_balance - COALESCE(h168.total_balance, c.total_balance)) / COALESCE(h168.total_balance, c.total_balance) * 100)
            END as hour_168_percentage,
            -- 720時間（30日）の変化
            c.total_balance - COALESCE(h720.total_balance, c.total_balance) as hour_720_change,
            CASE 
                WHEN COALESCE(h720.total_balance, c.total_balance) = 0 THEN 0
                ELSE ((c.total_balance - COALESCE(h720.total_balance, c.total_balance)) / COALESCE(h720.total_balance, c.total_balance) * 100)
            END as hour_720_percentage
        FROM current_totals c
        -- 1時間前のデータ
        LEFT JOIN xrpl_rich_list_summary h1 ON 
            c.grouped_label = h1.grouped_label AND 
            h1.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at > c.created_at - INTERVAL '70 minutes'
                AND created_at <= c.created_at - INTERVAL '45 minutes'
                ORDER BY created_at DESC 
                LIMIT 1
            )
        -- 3時間前のデータ
        LEFT JOIN xrpl_rich_list_summary h3 ON 
            c.grouped_label = h3.grouped_label AND 
            h3.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= c.created_at - INTERVAL '3 hours'
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
    INSERT INTO xrpl_rich_list_available_changes 
        (grouped_label, hours, balance_change, percentage_change, calculated_at)
    SELECT 
        grouped_label,
        1,
        hour_1_change,
        hour_1_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        grouped_label,
        3,
        hour_3_change,
        hour_3_percentage,
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
set statement_timeout = '120s'
as $$
begin
    -- 古いデータの削除
    DELETE FROM xrpl_rich_list
    WHERE snapshot_date < CURRENT_TIMESTAMP - INTERVAL '2 days';
    
    DELETE FROM xrpl_rich_list_summary
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '730 days';
end;
$$;

-- カテゴリ別の残高変更を更新する関数
CREATE OR REPLACE FUNCTION update_category_changes()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout = '60s'
AS $$
BEGIN
    -- 既存のデータを削除
    DELETE FROM xrpl_rich_list_category_changes WHERE TRUE;
    
    -- 新しいデータを挿入
    WITH current_totals AS (
        SELECT 
            c.category,
            SUM(count) as count,
            SUM(s.total_balance) as total_balance,
            SUM(s.total_escrow) as total_escrow,
            SUM(s.total_xrp) as total_xrp,
            s.created_at
        FROM xrpl_rich_list_summary s
        JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
        WHERE s.created_at = (
            SELECT created_at 
            FROM xrpl_rich_list_summary 
            ORDER BY created_at DESC 
            LIMIT 1
        )
        GROUP BY c.category, s.created_at
    ),
    period_changes AS (
        SELECT 
            c.category,
            c.count,
            c.total_balance,
            c.total_escrow,
            c.total_xrp,
            -- 1時間の変化
            c.total_xrp - COALESCE(h1.total_xrp, c.total_xrp) as hour_1_change,
            CASE 
                WHEN COALESCE(h1.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h1.total_xrp, c.total_xrp)) / COALESCE(h1.total_xrp, c.total_xrp) * 100)
            END as hour_1_percentage,
            -- 3時間の変化
            c.total_xrp - COALESCE(h3.total_xrp, c.total_xrp) as hour_3_change,
            CASE 
                WHEN COALESCE(h3.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h3.total_xrp, c.total_xrp)) / COALESCE(h3.total_xrp, c.total_xrp) * 100)
            END as hour_3_percentage,
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
        -- 1時間前のデータ
        LEFT JOIN (
            SELECT 
                c.category,
                SUM(s.total_xrp) as total_xrp
            FROM xrpl_rich_list_summary s
            JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
            WHERE s.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at > (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '70 minutes'
                AND created_at <= (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '45 minutes'
                ORDER BY created_at DESC 
                LIMIT 1
            )
            GROUP BY c.category
        ) h1 ON c.category = h1.category
        -- 3時間前のデータ
        LEFT JOIN (
            SELECT 
                c.category,
                SUM(s.total_xrp) as total_xrp
            FROM xrpl_rich_list_summary s
            JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
            WHERE s.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '3 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
            GROUP BY c.category
        ) h3 ON c.category = h3.category
        -- 24時間前のデータ
        LEFT JOIN (
            SELECT 
                c.category,
                SUM(s.total_xrp) as total_xrp
            FROM xrpl_rich_list_summary s
            JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
            WHERE s.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '24 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
            GROUP BY c.category
        ) h24 ON c.category = h24.category
        -- 168時間前のデータ
        LEFT JOIN (
            SELECT 
                c.category,
                SUM(s.total_xrp) as total_xrp
            FROM xrpl_rich_list_summary s
            JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
            WHERE s.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '168 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
            GROUP BY c.category
        ) h168 ON c.category = h168.category
        -- 720時間前のデータ
        LEFT JOIN (
            SELECT 
                c.category,
                SUM(s.total_xrp) as total_xrp
            FROM xrpl_rich_list_summary s
            JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
            WHERE s.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '720 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
            GROUP BY c.category
        ) h720 ON c.category = h720.category
    )
    INSERT INTO xrpl_rich_list_category_changes 
        (category, hours, count, total_balance, total_escrow, total_xrp, balance_change, percentage_change, calculated_at)
    SELECT 
        category,
        1,
        count,
        total_balance,
        total_escrow,
        total_xrp,
        hour_1_change,
        hour_1_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        category,
        3,
        count,
        total_balance,
        total_escrow,
        total_xrp,
        hour_3_change,
        hour_3_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        category,
        24,
        count,
        total_balance,
        total_escrow,
        total_xrp,
        hour_24_change,
        hour_24_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        category,
        168,
        count,
        total_balance,
        total_escrow,
        total_xrp,
        hour_168_change,
        hour_168_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        category,
        720,
        count,
        total_balance,
        total_escrow,
        total_xrp,
        hour_720_change,
        hour_720_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes;
END;
$$;

-- 国別の残高変更を更新する関数
CREATE OR REPLACE FUNCTION update_country_changes()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout = '60s'
AS $$
BEGIN
    -- 既存のデータを削除
    DELETE FROM xrpl_rich_list_country_changes WHERE TRUE;
    
    -- 新しいデータを挿入
    WITH current_totals AS (
        SELECT 
            c.country,
            SUM(count) as count,
            SUM(s.total_balance) as total_balance,
            SUM(s.total_escrow) as total_escrow,
            SUM(s.total_xrp) as total_xrp,
            s.created_at
        FROM xrpl_rich_list_summary s
        JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
        WHERE s.created_at = (
            SELECT created_at 
            FROM xrpl_rich_list_summary 
            ORDER BY created_at DESC 
            LIMIT 1
        )
        GROUP BY c.country, s.created_at
    ),
    period_changes AS (
        SELECT 
            c.country,
            c.count,
            c.total_balance,
            c.total_escrow,
            c.total_xrp,
            -- 1時間の変化
            c.total_xrp - COALESCE(h1.total_xrp, c.total_xrp) as hour_1_change,
            CASE 
                WHEN COALESCE(h1.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h1.total_xrp, c.total_xrp)) / COALESCE(h1.total_xrp, c.total_xrp) * 100)
            END as hour_1_percentage,
            -- 3時間の変化
            c.total_xrp - COALESCE(h3.total_xrp, c.total_xrp) as hour_3_change,
            CASE 
                WHEN COALESCE(h3.total_xrp, c.total_xrp) = 0 THEN 0
                ELSE ((c.total_xrp - COALESCE(h3.total_xrp, c.total_xrp)) / COALESCE(h3.total_xrp, c.total_xrp) * 100)
            END as hour_3_percentage,
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
        -- 1時間前のデータ
        LEFT JOIN (
            SELECT 
                c.country,
                SUM(s.total_xrp) as total_xrp,
                s.created_at
            FROM xrpl_rich_list_summary s
            JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
            WHERE s.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at > (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '70 minutes'
                AND created_at <= (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '45 minutes'
                ORDER BY created_at DESC 
                LIMIT 1
            )
            GROUP BY c.country, s.created_at
        ) h1 ON c.country = h1.country
        -- 3時間前のデータ
        LEFT JOIN (
            SELECT 
                c.country,
                SUM(s.total_xrp) as total_xrp,
                s.created_at
            FROM xrpl_rich_list_summary s
            JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
            WHERE s.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '3 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
            GROUP BY c.country, s.created_at
        ) h3 ON c.country = h3.country
        -- 24時間前のデータ
        LEFT JOIN (
            SELECT 
                c.country,
                SUM(s.total_xrp) as total_xrp,
                s.created_at
            FROM xrpl_rich_list_summary s
            JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
            WHERE s.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '24 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
            GROUP BY c.country, s.created_at
        ) h24 ON c.country = h24.country
        -- 168時間前のデータ
        LEFT JOIN (
            SELECT 
                c.country,
                SUM(s.total_xrp) as total_xrp,
                s.created_at
            FROM xrpl_rich_list_summary s
            JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
            WHERE s.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '168 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
            GROUP BY c.country, s.created_at
        ) h168 ON c.country = h168.country
        -- 720時間前のデータ
        LEFT JOIN (
            SELECT 
                c.country,
                SUM(s.total_xrp) as total_xrp,
                s.created_at
            FROM xrpl_rich_list_summary s
            JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
            WHERE s.created_at = (
                SELECT created_at 
                FROM xrpl_rich_list_summary 
                WHERE created_at <= (SELECT created_at FROM current_totals LIMIT 1) - INTERVAL '720 hours'
                ORDER BY created_at DESC 
                LIMIT 1
            )
            GROUP BY c.country, s.created_at
        ) h720 ON c.country = h720.country
    )
    INSERT INTO xrpl_rich_list_country_changes 
        (country, hours, count, total_balance, total_escrow, total_xrp, balance_change, percentage_change, calculated_at)
    SELECT 
        country,
        1,
        count,
        total_balance,
        total_escrow,
        total_xrp,
        hour_1_change,
        hour_1_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        country,
        3,
        count,
        total_balance,
        total_escrow,
        total_xrp,
        hour_3_change,
        hour_3_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        country,
        24,
        count,
        total_balance,
        total_escrow,
        total_xrp,
        hour_24_change,
        hour_24_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        country,
        168,
        count,
        total_balance,
        total_escrow,
        total_xrp,
        hour_168_change,
        hour_168_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes
    UNION ALL
    SELECT 
        country,
        720,
        count,
        total_balance,
        total_escrow,
        total_xrp,
        hour_720_change,
        hour_720_percentage,
        CURRENT_TIMESTAMP
    FROM period_changes;
END;
$$;

-- 更新用の関数を修正
CREATE OR REPLACE FUNCTION update_hourly_statistics()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    -- 古いデータの削除（3日以上前のデータを削除）
    DELETE FROM xrpl_rich_list_category_hourly 
    WHERE created_at < CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days';
    
    DELETE FROM xrpl_rich_list_country_hourly 
    WHERE created_at < CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days';
    
    DELETE FROM xrpl_rich_list_available_hourly 
    WHERE created_at < CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days';
    
    -- カテゴリごとの最新データを挿入
    INSERT INTO xrpl_rich_list_category_hourly 
        (grouped_label, count, total_balance, total_escrow, total_xrp, created_at)
    WITH latest_summary AS (
        SELECT *
        FROM xrpl_rich_list_summary s
        WHERE s.created_at >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days'
    )
    SELECT 
        c.category as grouped_label,
        SUM(s.count) as count,
        SUM(s.total_balance) as total_balance,
        SUM(s.total_escrow) as total_escrow,
        SUM(s.total_xrp) as total_xrp,
        date_trunc('hour', s.created_at AT TIME ZONE 'UTC') as created_at
    FROM latest_summary s
    JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
    GROUP BY c.category, date_trunc('hour', s.created_at AT TIME ZONE 'UTC')
    ON CONFLICT (grouped_label, created_at) 
    DO UPDATE SET
        count = EXCLUDED.count,
        total_balance = EXCLUDED.total_balance,
        total_escrow = EXCLUDED.total_escrow,
        total_xrp = EXCLUDED.total_xrp;
    
    -- 国ごとの最新データを挿入
    INSERT INTO xrpl_rich_list_country_hourly 
        (grouped_label, count, total_balance, total_escrow, total_xrp, created_at)
    WITH latest_summary AS (
        SELECT *
        FROM xrpl_rich_list_summary s
        WHERE s.created_at >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days'
    )
    SELECT 
        c.country as grouped_label,
        SUM(s.count) as count,
        SUM(s.total_balance) as total_balance,
        SUM(s.total_escrow) as total_escrow,
        SUM(s.total_xrp) as total_xrp,
        date_trunc('hour', s.created_at AT TIME ZONE 'UTC') as created_at
    FROM latest_summary s
    JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
    GROUP BY c.country, date_trunc('hour', s.created_at AT TIME ZONE 'UTC')
    ON CONFLICT (grouped_label, created_at) 
    DO UPDATE SET
        count = EXCLUDED.count,
        total_balance = EXCLUDED.total_balance,
        total_escrow = EXCLUDED.total_escrow,
        total_xrp = EXCLUDED.total_xrp;

    -- エスクロー抜きの最新データを挿入
    INSERT INTO xrpl_rich_list_available_hourly 
        (grouped_label, count, total_balance, total_escrow, total_xrp, created_at)
    WITH latest_summary AS (
        SELECT *
        FROM xrpl_rich_list_summary s
        WHERE s.created_at >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days'
    )
    SELECT 
        s.grouped_label,
        s.count,
        s.total_balance,
        s.total_escrow,
        s.total_balance as total_xrp,
        date_trunc('hour', s.created_at AT TIME ZONE 'UTC') as created_at
    FROM latest_summary s
    GROUP BY s.grouped_label, s.count, s.total_balance, s.total_escrow, date_trunc('hour', s.created_at AT TIME ZONE 'UTC')
    ON CONFLICT (grouped_label, created_at) 
    DO UPDATE SET
        count = EXCLUDED.count,
        total_balance = EXCLUDED.total_balance,
        total_escrow = EXCLUDED.total_escrow,
        total_xrp = EXCLUDED.total_xrp;
END;
$$;

-- 古いデータ削除用の関数
CREATE OR REPLACE FUNCTION delete_old_statistics()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    DELETE FROM xrpl_rich_list_category_hourly 
    WHERE created_at < CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days';
    
    DELETE FROM xrpl_rich_list_country_hourly 
    WHERE created_at < CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days';
    
    DELETE FROM xrpl_rich_list_available_hourly 
    WHERE created_at < CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days';
END;
$$;

-- カテゴリ統計更新用の関数
CREATE OR REPLACE FUNCTION update_category_statistics()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    INSERT INTO xrpl_rich_list_category_hourly 
        (grouped_label, count, total_balance, total_escrow, total_xrp, created_at)
    WITH latest_summary AS (
        SELECT *
        FROM xrpl_rich_list_summary s
        WHERE s.created_at >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days'
    )
    SELECT 
        c.category as grouped_label,
        SUM(s.count) as count,
        SUM(s.total_balance) as total_balance,
        SUM(s.total_escrow) as total_escrow,
        SUM(s.total_xrp) as total_xrp,
        date_trunc('hour', s.created_at AT TIME ZONE 'UTC') as created_at
    FROM latest_summary s
    JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
    GROUP BY c.category, date_trunc('hour', s.created_at AT TIME ZONE 'UTC')
    ON CONFLICT (grouped_label, created_at) 
    DO UPDATE SET
        count = EXCLUDED.count,
        total_balance = EXCLUDED.total_balance,
        total_escrow = EXCLUDED.total_escrow,
        total_xrp = EXCLUDED.total_xrp;
END;
$$;

-- 国別統計更新用の関数
CREATE OR REPLACE FUNCTION update_country_statistics()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    INSERT INTO xrpl_rich_list_country_hourly 
        (grouped_label, count, total_balance, total_escrow, total_xrp, created_at)
    WITH latest_summary AS (
        SELECT *
        FROM xrpl_rich_list_summary s
        WHERE s.created_at >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days'
    )
    SELECT 
        c.country as grouped_label,
        SUM(s.count) as count,
        SUM(s.total_balance) as total_balance,
        SUM(s.total_escrow) as total_escrow,
        SUM(s.total_xrp) as total_xrp,
        date_trunc('hour', s.created_at AT TIME ZONE 'UTC') as created_at
    FROM latest_summary s
    JOIN xrpl_rich_list_categories c ON s.grouped_label = c.grouped_label
    GROUP BY c.country, date_trunc('hour', s.created_at AT TIME ZONE 'UTC')
    ON CONFLICT (grouped_label, created_at) 
    DO UPDATE SET
        count = EXCLUDED.count,
        total_balance = EXCLUDED.total_balance,
        total_escrow = EXCLUDED.total_escrow,
        total_xrp = EXCLUDED.total_xrp;
END;
$$;

-- 利用可能額統計更新用の関数
CREATE OR REPLACE FUNCTION update_available_statistics()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout = '60s'
AS $$
BEGIN
    INSERT INTO xrpl_rich_list_available_hourly 
        (grouped_label, count, total_balance, total_escrow, total_xrp, created_at)
    SELECT 
        s.grouped_label,
        s.count,
        s.total_balance,
        s.total_escrow,
        s.total_balance as total_xrp,
        date_trunc('hour', s.created_at AT TIME ZONE 'UTC') as created_at
    FROM xrpl_rich_list_summary s
    WHERE s.created_at >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '3 days'
    GROUP BY s.grouped_label, s.count, s.total_balance, s.total_escrow, date_trunc('hour', s.created_at AT TIME ZONE 'UTC')
    ON CONFLICT (grouped_label, created_at) 
    DO UPDATE SET
        count = EXCLUDED.count,
        total_balance = EXCLUDED.total_balance,
        total_escrow = EXCLUDED.total_escrow,
        total_xrp = EXCLUDED.total_xrp;
END;
$$;

-- ANALYZE実行用の関数
CREATE OR REPLACE FUNCTION analyze_rich_list_tables()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout = '60s'
AS $$
BEGIN
    -- 統計情報の更新（ANALYZE のみ）
    ANALYZE xrpl_rich_list_summary;
    ANALYZE xrpl_rich_list;
    ANALYZE xrpl_rich_list_changes;
    ANALYZE xrpl_rich_list_available_changes;
    ANALYZE xrpl_rich_list_category_changes;
    ANALYZE xrpl_rich_list_country_changes;
    ANALYZE xrpl_rich_list_available_hourly;
    ANALYZE xrpl_rich_list_category_hourly;
    ANALYZE xrpl_rich_list_country_hourly;
END;
$$;

CREATE OR REPLACE FUNCTION get_significant_changes(
    percentage_threshold FLOAT,
    amount_threshold FLOAT
) 
RETURNS TABLE (
    grouped_label VARCHAR(255),
    change_1h NUMERIC,
    percentage_1h NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.grouped_label,
        c.change_1h,
        c.percentage_1h
    FROM xrpl_rich_list_summary_with_total_changes c
    WHERE c.change_1h IS NOT NULL
    AND ABS(c.percentage_1h) >= percentage_threshold
    AND ABS(c.change_1h) >= amount_threshold
    ORDER BY ABS(c.percentage_1h) DESC
    LIMIT 5;
END;
$$ LANGUAGE plpgsql;
