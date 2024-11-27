BEGIN
    -- Ki?m tra n?u b?ng TEMP_MATLL t?n t?i, thì xóa di
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE TEMP_MATLL';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN -- L?i ORA-00942: Table does not exist
                RAISE;
            END IF;
    END;

    -- T?o l?i b?ng TEMP_MATLL
    EXECUTE IMMEDIATE '
        CREATE GLOBAL TEMPORARY TABLE TEMP_MATLL (
            TEN_TRAM VARCHAR2(100),
            TEN_DONVI VARCHAR2(100),
            TYPE NUMBER, -- Lo?i m?ng (2G, 3G, 4G)
            LOAI NUMBER, -- Lo?i tr?m (1: Có MPD, 5: Không có MPD)
            SO_SU_CO NUMBER,
            TONG_SO NUMBER,
            BAT_DAU TIMESTAMP,
            KET_THUC TIMESTAMP,
            TUAN NUMBER,
            NGAY DATE,
            DIEM NUMBER,
            TG_MLL_CO_MPD NUMBER,
            TG_MLL_KHONG_MPD NUMBER,
            SO_SU_CO_CO_MPD NUMBER,
            SO_SU_CO_KHONG_MPD NUMBER,
            NGAY_CUOI DATE -- C?t ngày cu?i
        ) ON COMMIT DELETE ROWS';
END;
/
CREATE OR REPLACE FUNCTION calculate_ngay_cuoi(
    ngay DATE,
    tong_so NUMBER
) RETURN DATE IS
BEGIN
    -- N?u m?t di?n <= 1 ngày (1440 phút), ngày cu?i là ngày b?t d?u
    IF tong_so <= 1440 THEN
        RETURN ngay;
    ELSE
        -- N?u m?t di?n > 1 ngày, tính s? ngày thêm
        RETURN ngay + FLOOR(tong_so / 1440);
    END IF;
END calculate_ngay_cuoi;
/
CREATE OR REPLACE PROCEDURE calculate_report_data(
    p_tu_tuan IN NUMBER,
    p_den_tuan IN NUMBER,
    p_tu_ngay IN DATE,
    p_den_ngay IN DATE
) IS
BEGIN
    -- Ki?m tra và xóa b?ng TEMP_MATLL n?u t?n t?i
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE TEMP_MATLL';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN -- L?i ORA-00942: Table does not exist
                RAISE;
            END IF;
    END;

    -- T?o l?i b?ng TEMP_MATLL
    EXECUTE IMMEDIATE '
        CREATE GLOBAL TEMPORARY TABLE TEMP_MATLL (
            TEN_TRAM VARCHAR2(100),
            TEN_DONVI VARCHAR2(100),
            TYPE NUMBER, -- Lo?i m?ng (2G, 3G, 4G)
            LOAI NUMBER, -- Lo?i tr?m (1: Có MPD, 5: Không có MPD)
            SO_SU_CO NUMBER,
            TONG_SO NUMBER,
            BAT_DAU TIMESTAMP,
            KET_THUC TIMESTAMP,
            TUAN NUMBER,
            NGAY DATE,
            DIEM NUMBER,
            TG_MLL_CO_MPD NUMBER,
            TG_MLL_KHONG_MPD NUMBER,
            SO_SU_CO_CO_MPD NUMBER,
            SO_SU_CO_KHONG_MPD NUMBER,
            NGAY_CUOI DATE -- C?t ngày cu?i
        ) ON COMMIT DELETE ROWS';

    -- Ph?n còn l?i c?a procedure...
    DELETE FROM TEMP_MATLL;

    INSERT INTO TEMP_MATLL (TEN_TRAM, TEN_DONVI, TYPE, LOAI, TONG_SO, BAT_DAU, KET_THUC, TUAN, NGAY, NGAY_CUOI)
    SELECT 
        t.TEN_TRAM, 
        t.TEN_DONVI, 
        t.TYPE, 
        m.LOAI, 
        m.TONG_SO, 
        TO_TIMESTAMP(m.NGAY || ' ' || m.BAT_DAU, 'DD/MM/YYYY HH24:MI:SS'), 
        TO_TIMESTAMP(m.NGAY || ' ' || m.KET_THUC, 'DD/MM/YYYY HH24:MI:SS'), 
        m.TUAN, 
        m.NGAY,
        calculate_ngay_cuoi(m.NGAY, m.TONG_SO) -- Tính ngày cu?i
    FROM TRAM_BTS_THANG t
    FULL OUTER JOIN MATLL m 
    ON UPPER(t.TEN_TRAM) = UPPER(m.TEN_TRAM)
    WHERE (
        (p_tu_ngay IS NOT NULL AND p_den_ngay IS NOT NULL AND m.NGAY BETWEEN p_tu_ngay AND p_den_ngay)
        OR 
        (p_tu_tuan IS NOT NULL AND p_den_tuan IS NOT NULL AND m.TUAN BETWEEN p_tu_tuan AND p_den_tuan)
    );
END calculate_report_data;
/
CREATE OR REPLACE PROCEDURE get_report_data(
    p_tu_tuan IN NUMBER,
    p_den_tuan IN NUMBER,
    p_tu_ngay IN DATE,
    p_den_ngay IN DATE,
    p_result OUT SYS_REFCURSOR
)
IS
BEGIN
    -- Tính toán d? li?u tru?c khi l?y k?t qu?
    calculate_report_data(p_tu_tuan, p_den_tuan, p_tu_ngay, p_den_ngay);

    -- Tr? v? k?t qu?
    OPEN p_result FOR
    SELECT 
        d.TEN_DONVI,
        l.LOAI_MANG,
        NVL(COUNT(t.TEN_TRAM), 0) AS SO_TRAM,
        NVL(SUM(t.TONG_SO), 0) AS TONG_TG_MLL,
        NVL(SUM(t.DIEM), 0) AS TONG_DIEM,
        MIN(t.NGAY_CUOI) AS NGAY_CUOI_MIN,
        MAX(t.NGAY_CUOI) AS NGAY_CUOI_MAX
    FROM 
        (SELECT DISTINCT TEN_DONVI FROM TRAM_BTS_THANG) d
    CROSS JOIN 
        (SELECT '2G' AS LOAI_MANG FROM DUAL 
         UNION ALL 
         SELECT '3G' FROM DUAL 
         UNION ALL 
         SELECT '4G' FROM DUAL) l
    LEFT JOIN TEMP_MATLL t 
    ON d.TEN_DONVI = t.TEN_DONVI 
    AND l.LOAI_MANG = CASE 
                        WHEN t.TYPE = 2 THEN '2G'
                        WHEN t.TYPE = 3 THEN '3G'
                        WHEN t.TYPE = 4 THEN '4G'
                     END
    GROUP BY d.TEN_DONVI, l.LOAI_MANG
    ORDER BY d.TEN_DONVI, l.LOAI_MANG;
END get_report_data;
/
