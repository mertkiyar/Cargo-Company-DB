DO $$
DECLARE
    -- ID ve Key Değişkenleri
    v_address_id INT;
    v_start_branch_id INT;
    v_dest_branch_id INT;
    v_employee_id INT;
    v_customer_id INT;
    v_sender_address_id INT;
    v_receiver_address_id INT;
    v_receiver_customer_id INT;
    
    -- STRING ANAHTARLAR
    v_tracking_number CHAR(12); 
    v_package_barcode CHAR(14);
    v_invoice_number CHAR(14);
    
    -- Operasyonel ID'ler
    v_delivery_id BIGINT;
    v_vehicle_id INT;
    v_courier_id INT;
    
    -- Zamanlama ve Oran Değişkenleri
    v_shipment_created_at TIMESTAMP;
    v_log_date TIMESTAMP;
    v_batch_date TIMESTAMP;
    v_delivery_duration INTERVAL;
    v_random_percent INT; 
    
    -- Paket Adedi ve Boyutları
    v_pkg_count INT;
    v_pkg_loop INT;
    v_weight DECIMAL(4,2);
    v_height DECIMAL(5,2);
    v_width DECIMAL(5,2);
    v_length DECIMAL(5,2);
    
    -- Araç Mantığı
    v_vehicle_model VARCHAR(32);
    v_vol_cap DECIMAL(6,2);
    v_wgt_cap DECIMAL(6,2);
    
    -- Müşteri Mantığı
    v_gender VARCHAR(6);
    v_first_name VARCHAR(100);
    
    -- Ödeme Yöntemi
    v_payment_method VARCHAR(16);
    v_payment_status PAYMENT_STATUS;
    
    -- Kurye Döngüsü
    v_courier_limit INT;
    
    -- Adres Mantığı
    v_city_selection INT;
    v_suffix_selection INT; 
    v_addr_count INT;
    v_addr_loop INT;
    v_address_title VARCHAR(20);
    v_city_name VARCHAR(32);
    v_district_name VARCHAR(32);
    v_district_pool TEXT[]; 
    v_neighborhood_name VARCHAR(50);
    v_branch_suffix VARCHAR(20);
    
    -- Batching Değişkenleri
    rec RECORD;
    v_current_branch_id INT := -1;
    v_is_delivery_active BOOLEAN := FALSE; 
    v_is_delivery_failed BOOLEAN := FALSE;
    
    -- Durum Değişkenleri
    v_target_status PACKAGE_STATUS; 
    v_cust_type CUSTOMER_TYPE;
    v_pkg_type PACKAGE_TYPE;
    
    -- Fiyat
    v_random_price DECIMAL(9,2);
    
    -- İSİM HAVUZLARI
    male_names TEXT[] := ARRAY['Ahmet', 'Mehmet', 'Ali', 'Veli', 'Mustafa', 'Burak', 'Cem', 'Can', 'Oğuz', 'Kaan', 'Mert', 'Emre', 'Kerem', 'Volkan', 'Sinan', 'Yusuf', 'Eren', 'Arda', 'Barış'];
    female_names TEXT[] := ARRAY['Ayşe', 'Fatma', 'Zeynep', 'Elif', 'Selin', 'Gamze', 'Seda', 'Esra', 'Buse', 'Merve', 'İrem', 'Ebru', 'Derya', 'Ceren', 'Aleyna', 'Beyza', 'Kübra'];
    last_names TEXT[] := ARRAY['Yılmaz', 'Kaya', 'Demir', 'Çelik', 'Şahin', 'Yıldız', 'Öztürk', 'Aydın', 'Özdemir', 'Arslan', 'Doğan', 'Kılıç', 'Koç', 'Kurt', 'Kara', 'Şimşek', 'Polat'];
    
    comp_prefixes TEXT[] := ARRAY['Anadolu', 'Avrupa', 'Asya', 'Ege', 'Marmara', 'Akdeniz', 'Karadeniz', 'Kuzey', 'Güney', 'Doğu', 'Batı', 'Yıldız', 'Güneş', 'Mavi', 'Yeşil', 'Kırmızı', 'Beyaz', 'Siyah', 'Demir', 'Çelik', 'Altın', 'Gümüş', 'Mega', 'Ultra', 'Global', 'Dinamik', 'Elit', 'Prestij', 'Vizyon', 'Hedef', 'Zirve', 'Sistem', 'Teknik', 'Modern'];
    comp_sectors TEXT[] := ARRAY['Bilişim', 'Yazılım', 'Teknoloji', 'Gıda', 'Tarım', 'İnşaat', 'Yapı', 'Mimarlık', 'Mühendislik', 'Lojistik', 'Nakliyat', 'Kargo', 'Otomotiv', 'Tekstil', 'Moda', 'Sağlık', 'Medikal', 'Eczacılık', 'Turizm', 'Otelcilik', 'Eğitim', 'Yayıncılık', 'Medya', 'Reklam', 'Pazarlama', 'Danışmanlık', 'Hukuk', 'Finans', 'Sigorta', 'Enerji', 'Plastik', 'Kimya', 'Ambalaj', 'Güvenlik', 'Temizlik'];
    comp_suffixes TEXT[] := ARRAY['A.Ş.', 'Ltd. Şti.', 'Holding', 'Grup', 'Sanayi ve Ticaret A.Ş.', 'Hizmetleri'];

    neighboorhoods TEXT[] := ARRAY['Atatürk Mah.', 'Cumhuriyet Mah.', 'İnönü Mah.', 'Fatih Sultan Mah.', 'Merkez Mah.', 'Yavuz Selim Mah.', 'Barbaros Mah.', 'Hürriyet Mah.', 'İstiklal Mah.', 'Yeni Mah.', 'Bahçelievler Mah.', 'Gültepe Mah.'];
    streets TEXT[] := ARRAY['Atatürk Cad.', 'Cumhuriyet Cad.', 'Lale Sok.', 'Gül Sok.', 'Menekşe Sok.', 'İnönü Cad.', 'Fatih Cad.', 'Okul Sok.', 'Park Sok.', 'Karanfil Sok.', 'Papatya Sok.', 'Çiğdem Sok.'];
    
    branch_suffixes TEXT[] := ARRAY['Merkez', 'Çarşı', 'Sanayi', 'Meydan', 'Kurumsal', 'Liman', 'Havalimanı', 'Organize', 'Bulvar', 'Cadde'];

BEGIN
    -- 1. TEMİZLİK
    RAISE NOTICE 'Tablolar temizleniyor...';
    TRUNCATE TABLE PAYMENT, INVOICE, HAS_PACKAGE, DELIVERY, TRACKING_LOG, PACKAGE, SHIPMENT, HAS_ADDRESS, CORPORATE_CUSTOMER, INDIVIDUAL_CUSTOMER, CUSTOMER, BRANCH_STAFF, COURIER, EMPLOYEE, BRANCH, ADDRESS, VEHICLE RESTART IDENTITY CASCADE;

    RAISE NOTICE 'Statik veriler (Araç, Şube, Personel) oluşturuluyor...';

    -- 2. ARAÇLAR (100 Adet)
    FOR i IN 1..100 LOOP
        v_vehicle_model := (ARRAY['Ford Transit', 'Fiat Doblo', 'Renault Master', 'Motosiklet', 'Citroen Berlingo', 'Volkswagen Transporter', 'Mercedes Sprinter', 'Peugeot Partner', 'Honda Activa', 'Yamaha XMAX'])[floor(random()*10)+1];
        
        IF v_vehicle_model IN ('Motosiklet', 'Honda Activa', 'Yamaha XMAX') THEN
            v_vol_cap := (random() * 0.1 + 0.1); v_wgt_cap := (random() * 20 + 30);   
        ELSIF v_vehicle_model IN ('Fiat Doblo', 'Citroen Berlingo', 'Peugeot Partner') THEN
            v_vol_cap := (random() * 1 + 3);     v_wgt_cap := (random() * 200 + 600); 
        ELSIF v_vehicle_model IN ('Ford Transit', 'Volkswagen Transporter') THEN
            v_vol_cap := (random() * 2 + 10);    v_wgt_cap := (random() * 300 + 1200);
        ELSE -- Renault Master, Mercedes Sprinter
            v_vol_cap := (random() * 3 + 13);    v_wgt_cap := (random() * 500 + 1500);
        END IF;

        INSERT INTO VEHICLE (PLATE, MODEL, VOLUME_CAPACITY, WEIGHT_CAPACITY, CREATED_AT)
        VALUES (
            CONCAT('34', chr(65 + (i % 26)), chr(66 + (i % 26)), 100 + i), 
            v_vehicle_model, v_vol_cap::numeric(6,2), v_wgt_cap::numeric(6,2), 
            CURRENT_DATE - (random() * 365 * interval '1 day')
        );
    END LOOP;

    -- 3. ŞUBELER VE PERSONEL (50 Şube)
    FOR i IN 1..50 LOOP
        v_city_selection := ((i - 1) % 7) + 1; 
        
        CASE v_city_selection
            WHEN 1 THEN v_city_name := 'İstanbul'; v_district_pool := ARRAY['Avcılar', 'Beşiktaş', 'Kadıköy', 'Şişli', 'Üsküdar', 'Maltepe', 'Fatih', 'Esenyurt'];
            WHEN 2 THEN v_city_name := 'Ankara'; v_district_pool := ARRAY['Çankaya', 'Keçiören', 'Yenimahalle', 'Mamak', 'Etimesgut'];
            WHEN 3 THEN v_city_name := 'İzmir'; v_district_pool := ARRAY['Konak', 'Bornova', 'Karşıyaka', 'Buca', 'Gaziemir'];
            WHEN 4 THEN v_city_name := 'Bursa'; v_district_pool := ARRAY['Nilüfer', 'Osmangazi', 'Yıldırım'];
            WHEN 5 THEN v_city_name := 'Antalya'; v_district_pool := ARRAY['Muratpaşa', 'Kepez', 'Konyaaltı'];
            WHEN 6 THEN v_city_name := 'Adana'; v_district_pool := ARRAY['Seyhan', 'Çukurova', 'Yüreğir'];
            ELSE        v_city_name := 'Gaziantep'; v_district_pool := ARRAY['Şahinbey', 'Şehitkamil'];
        END CASE;

        v_district_name := v_district_pool[floor(random() * array_length(v_district_pool, 1)) + 1];
        v_suffix_selection := ((i - 1) % 10) + 1; 
        v_branch_suffix := branch_suffixes[v_suffix_selection];

        INSERT INTO ADDRESS (NEIGHBORHOOD, STREET, APARTMENT_NUMBER, STATE, CITY, COUNTRY, ZIP)
        VALUES (
            neighboorhoods[floor(random()*array_length(neighboorhoods, 1))+1],
            streets[floor(random()*array_length(streets, 1))+1],
            (floor(random()*100)+1)::text,
            v_district_name, v_city_name, 'Türkiye', 
            (10000 + floor(random()*80000))::text
        ) RETURNING ADDRESS_ID INTO v_address_id;

        INSERT INTO BRANCH (BRANCH_NAME, BRANCH_PHONE, BRANCH_ADDRESS_ID)
        VALUES (
            CONCAT(v_city_name, ' ', v_district_name, ' ', v_branch_suffix, ' Şubesi'), 
            CONCAT('05', floor(random()*100 + 300), floor(random()*1000000 + 1000000)), v_address_id
        ) RETURNING BRANCH_ID INTO v_start_branch_id; 

        -- Branch Staff
        FOR k IN 1..3 LOOP
            v_first_name := (male_names || female_names)[floor(random()*array_length(male_names || female_names, 1))+1];
            INSERT INTO EMPLOYEE (FIRST_NAME, LAST_NAME, SSN, BRANCH_ID, TYPE) 
            VALUES (
                v_first_name, last_names[floor(random()*array_length(last_names, 1))+1], 
                (10000000000 + floor(random() * 89999999999))::text, v_start_branch_id, 'BRANCH_STAFF'
            ) RETURNING EMPLOYEE_ID INTO v_employee_id;
            INSERT INTO BRANCH_STAFF VALUES (v_employee_id, (ARRAY['Manager', 'Desk Clerk', 'Security', 'Cleaner'])[floor(random()*4)+1]);
        END LOOP;
        
        -- Courier
        IF (i % 2) = 0 THEN v_courier_limit := 4; ELSE v_courier_limit := 5; END IF;
        FOR k IN 1..v_courier_limit LOOP
            v_first_name := (male_names || female_names)[floor(random()*array_length(male_names || female_names, 1))+1];
            INSERT INTO EMPLOYEE (FIRST_NAME, LAST_NAME, SSN, BRANCH_ID, TYPE) 
            VALUES (
                v_first_name, last_names[floor(random()*array_length(last_names, 1))+1], 
                (10000000000 + floor(random() * 89999999999))::text, v_start_branch_id, 'COURIER'
            ) RETURNING EMPLOYEE_ID INTO v_employee_id;
            INSERT INTO COURIER VALUES (v_employee_id, 'B Sınıfı', 'AVAILABLE');
        END LOOP;
    END LOOP;

    -- 4. MÜŞTERİLER (1000 Adet)
    FOR i IN 1..1000 LOOP
        INSERT INTO CUSTOMER (PHONE, EMAIL, TYPE)
        VALUES (
            CONCAT('05', floor(random()*90 + 30)::text, floor(random()*1000000 + 1000000)::text),
            CONCAT('user', i, '@mail.com'),
            CASE WHEN random() > 0.8 THEN 'CORPORATE_CUSTOMER'::CUSTOMER_TYPE ELSE 'INDIVIDUAL_CUSTOMER'::CUSTOMER_TYPE END
        ) RETURNING CUSTOMER_ID, TYPE INTO v_customer_id, v_cust_type;

        IF v_cust_type = 'INDIVIDUAL_CUSTOMER' THEN
            IF random() > 0.5 THEN v_gender := 'Male'; v_first_name := male_names[floor(random()*array_length(male_names, 1))+1];
            ELSE v_gender := 'Female'; v_first_name := female_names[floor(random()*array_length(female_names, 1))+1]; END IF;

            INSERT INTO INDIVIDUAL_CUSTOMER VALUES (
                v_customer_id, v_first_name, last_names[floor(random()*array_length(last_names, 1))+1], 
                v_gender, (10000000000 + floor(random() * 89999999999))::text, 
                DATE '2007-12-31' - (floor(random() * 18000) * interval '1 day')
            );
        ELSE
            INSERT INTO CORPORATE_CUSTOMER VALUES (
                v_customer_id, 
                CONCAT(comp_prefixes[floor(random()*array_length(comp_prefixes, 1))+1], ' ', comp_sectors[floor(random()*array_length(comp_sectors, 1))+1], ' ', comp_suffixes[floor(random()*array_length(comp_suffixes, 1))+1]), 
                (1000000000 + floor(random() * 8999999999))::text
            );
        END IF;

        -- Adres Döngüsü
        v_random_percent := floor(random() * 100) + 1;
        IF v_random_percent <= 70 THEN v_addr_count := 1;
        ELSIF v_random_percent <= 95 THEN v_addr_count := 2;
        ELSE v_addr_count := 3; END IF;

        FOR v_addr_loop IN 1..v_addr_count LOOP
            IF v_addr_loop = 1 THEN v_address_title := 'Ev';
            ELSIF v_addr_loop = 2 THEN v_address_title := 'İş';
            ELSE v_address_title := 'Diğer'; END IF;

            v_city_selection := floor(random() * 7) + 1;
            CASE v_city_selection
                WHEN 1 THEN v_city_name := 'İstanbul'; v_district_pool := ARRAY['Avcılar', 'Beşiktaş', 'Kadıköy', 'Şişli', 'Üsküdar', 'Maltepe', 'Fatih', 'Esenyurt'];
                WHEN 2 THEN v_city_name := 'Ankara'; v_district_pool := ARRAY['Çankaya', 'Keçiören', 'Yenimahalle', 'Mamak', 'Etimesgut'];
                WHEN 3 THEN v_city_name := 'İzmir'; v_district_pool := ARRAY['Konak', 'Bornova', 'Karşıyaka', 'Buca', 'Gaziemir'];
                WHEN 4 THEN v_city_name := 'Bursa'; v_district_pool := ARRAY['Nilüfer', 'Osmangazi', 'Yıldırım'];
                WHEN 5 THEN v_city_name := 'Antalya'; v_district_pool := ARRAY['Muratpaşa', 'Kepez', 'Konyaaltı'];
                WHEN 6 THEN v_city_name := 'Adana'; v_district_pool := ARRAY['Seyhan', 'Çukurova', 'Yüreğir'];
                ELSE        v_city_name := 'Gaziantep'; v_district_pool := ARRAY['Şahinbey', 'Şehitkamil'];
            END CASE;
            v_district_name := v_district_pool[floor(random() * array_length(v_district_pool, 1)) + 1];

            INSERT INTO ADDRESS (NEIGHBORHOOD, STREET, APARTMENT_NUMBER, STATE, CITY, COUNTRY, ZIP)
            VALUES (
                neighboorhoods[floor(random()*array_length(neighboorhoods, 1))+1], streets[floor(random()*array_length(streets, 1))+1], 
                (floor(random()*100)+1)::text, v_district_name, v_city_name, 'Türkiye', (10000 + floor(random()*80000))::text
            ) RETURNING ADDRESS_ID INTO v_address_id;
            
            INSERT INTO HAS_ADDRESS VALUES (v_customer_id, v_address_id, v_address_title);
        END LOOP;
    END LOOP;

    -- =================================================================================
    -- AŞAMA 1: GÖNDERİ VE PAKET OLUŞTURMA
    -- =================================================================================
    RAISE NOTICE 'Paketler oluşturuluyor (Hedef: 3000 Shipment)...';
    
    FOR i IN 1..3000 LOOP 
        v_random_percent := floor(random() * 100) + 1;
        IF v_random_percent <= 50 THEN v_pkg_count := 1;
        ELSIF v_random_percent <= 80 THEN v_pkg_count := 2;
        ELSE v_pkg_count := 3; END IF;

        v_start_branch_id := (SELECT BRANCH_ID FROM BRANCH ORDER BY random() LIMIT 1);
        LOOP
            v_dest_branch_id := (SELECT BRANCH_ID FROM BRANCH ORDER BY random() LIMIT 1);
            EXIT WHEN v_dest_branch_id != v_start_branch_id;
        END LOOP;

        v_customer_id := (SELECT CUSTOMER_ID FROM CUSTOMER ORDER BY random() LIMIT 1);
        v_sender_address_id := (SELECT ADDRESS_ID FROM HAS_ADDRESS WHERE CUSTOMER_ID = v_customer_id ORDER BY random() LIMIT 1);
        v_receiver_customer_id := (SELECT CUSTOMER_ID FROM CUSTOMER WHERE CUSTOMER_ID != v_customer_id ORDER BY random() LIMIT 1);
        
        -- Alıcı Adresi (Rastgele)
        v_city_selection := floor(random() * 7) + 1;
        CASE v_city_selection
            WHEN 1 THEN v_city_name := 'İstanbul'; v_district_pool := ARRAY['Avcılar', 'Beşiktaş', 'Kadıköy', 'Şişli', 'Üsküdar', 'Maltepe', 'Fatih', 'Esenyurt'];
            WHEN 2 THEN v_city_name := 'Ankara'; v_district_pool := ARRAY['Çankaya', 'Keçiören', 'Yenimahalle', 'Mamak', 'Etimesgut'];
            WHEN 3 THEN v_city_name := 'İzmir'; v_district_pool := ARRAY['Konak', 'Bornova', 'Karşıyaka', 'Buca', 'Gaziemir'];
            WHEN 4 THEN v_city_name := 'Bursa'; v_district_pool := ARRAY['Nilüfer', 'Osmangazi', 'Yıldırım'];
            WHEN 5 THEN v_city_name := 'Antalya'; v_district_pool := ARRAY['Muratpaşa', 'Kepez', 'Konyaaltı'];
            WHEN 6 THEN v_city_name := 'Adana'; v_district_pool := ARRAY['Seyhan', 'Çukurova', 'Yüreğir'];
            ELSE        v_city_name := 'Gaziantep'; v_district_pool := ARRAY['Şahinbey', 'Şehitkamil'];
        END CASE;
        v_district_name := v_district_pool[floor(random() * array_length(v_district_pool, 1)) + 1];

        INSERT INTO ADDRESS (NEIGHBORHOOD, STREET, APARTMENT_NUMBER, STATE, CITY, COUNTRY, ZIP)
        VALUES (neighboorhoods[floor(random()*8)+1], streets[floor(random()*7)+1], (floor(random()*100)+1)::text, v_district_name, v_city_name, 'Türkiye', (10000 + floor(random()*80000))::text) 
        RETURNING ADDRESS_ID INTO v_receiver_address_id;

        -- Hedef Durum ve TUTARLI TARİH
        v_random_percent := floor(random() * 100) + 1;
        IF v_random_percent <= 60 THEN v_target_status := 'DELIVERED';
        ELSIF v_random_percent <= 75 THEN IF random() > 0.5 THEN v_target_status := 'RETURNING'; ELSE v_target_status := 'RETURNED'; END IF;
        ELSIF v_random_percent <= 85 THEN v_target_status := (ARRAY['IN_TRANSIT', 'RECEIVED_AT_BRANCH', 'ARRIVED_DESTINATION'])[floor(random()*3)+1];
        ELSIF v_random_percent <= 95 THEN v_target_status := 'OUT_FOR_DELIVERY';
        ELSE v_target_status := 'DELIVERY_FAILED'; END IF;

        -- Aktif kargolar için tarih en fazla 4 gün önce olabilir (5 Gün kuralına takılmasın)
        IF v_target_status IN ('OUT_FOR_DELIVERY', 'IN_TRANSIT', 'ARRIVED_DESTINATION', 'RECEIVED_AT_BRANCH', 'CREATED') THEN
             v_shipment_created_at := NOW() - (floor(random()*4) * interval '1 day') - (floor(random()*24) * interval '1 hour'); 
        ELSE
             v_shipment_created_at := NOW() - (floor(random()*60 + 5) * interval '1 day'); 
        END IF;

        v_tracking_number := CONCAT('TR', floor(random()*9000000000 + 1000000000)::text);

        INSERT INTO SHIPMENT (TRACKING_NUMBER, CREATED_AT, SENDER_ADDRESS_ID, RECEIVER_ADDRESS_ID, CUSTOMER_ID, RECEIVER_ID)
        VALUES (v_tracking_number, v_shipment_created_at, v_sender_address_id, v_receiver_address_id, v_customer_id, v_receiver_customer_id);

        FOR v_pkg_loop IN 1..v_pkg_count LOOP
            v_package_barcode := CONCAT('PKG', floor(random()*90000000000 + 10000000000)::text);
            v_pkg_type := (enum_range(NULL::PACKAGE_TYPE))[floor(random()*6)+1];
            v_weight := (random() * 29.5 + 0.5)::numeric(4,2); 
            v_height := (random() * 90 + 10)::numeric(5,2); v_width := (random() * 90 + 10)::numeric(5,2); v_length := (random() * 90 + 10)::numeric(5,2); 

            INSERT INTO PACKAGE (PACKAGE_BARCODE, TYPE, WEIGHT, HEIGHT, WIDTH, LENGTH, TRACKING_NUMBER, BRANCH_ID, STATUS)
            VALUES (v_package_barcode, v_pkg_type, v_weight, v_height, v_width, v_length, v_tracking_number, v_start_branch_id, v_target_status);

            v_log_date := v_shipment_created_at; 
            INSERT INTO TRACKING_LOG VALUES (DEFAULT, v_log_date, 'CREATED', v_start_branch_id, v_package_barcode);
            IF v_target_status = 'CREATED' THEN CONTINUE; END IF;

            v_log_date := v_log_date + interval '6 hours' + (random() * interval '4 hours');
            INSERT INTO TRACKING_LOG VALUES (DEFAULT, v_log_date, 'RECEIVED_AT_BRANCH', v_start_branch_id, v_package_barcode);
            IF v_target_status = 'RECEIVED_AT_BRANCH' THEN CONTINUE; END IF;

            v_log_date := v_log_date + (random() * interval '12 hours');
            INSERT INTO TRACKING_LOG VALUES (DEFAULT, v_log_date, 'IN_TRANSIT', NULL, v_package_barcode);
            IF v_target_status = 'IN_TRANSIT' THEN CONTINUE; END IF;

            v_log_date := v_log_date + (random() * interval '12 hours');
            INSERT INTO TRACKING_LOG VALUES (DEFAULT, v_log_date, 'ARRIVED_DESTINATION', v_dest_branch_id, v_package_barcode);
            UPDATE PACKAGE SET BRANCH_ID = v_dest_branch_id WHERE PACKAGE_BARCODE = v_package_barcode;
        END LOOP;
    END LOOP;

    -- =================================================================================
    -- AŞAMA 2: DAĞITIM VE FATURALAMA
    -- =================================================================================
    RAISE NOTICE 'Dağıtım planları (Deliveries) oluşturuluyor...';

    FOR rec IN 
        SELECT S.TRACKING_NUMBER, MAX(P.BRANCH_ID) as BRANCH_ID, MAX(P.STATUS) as STATUS, MAX(S.CREATED_AT) as SHIP_DATE
        FROM PACKAGE P
        JOIN SHIPMENT S ON P.TRACKING_NUMBER = S.TRACKING_NUMBER
        GROUP BY S.TRACKING_NUMBER
    LOOP
        v_invoice_number := CONCAT('INV', floor(random()*90000000000 + 10000000000)::text);
        v_random_price := (random() * 500 + 40)::numeric(9,2);
        
        v_random_percent := floor(random() * 100) + 1;
        IF v_random_percent <= 70 THEN v_payment_method := 'CREDIT_CARD'; v_payment_status := 'SUCCESS';
        ELSIF v_random_percent <= 85 THEN v_payment_method := 'CASH'; v_payment_status := 'PENDING';
        ELSIF v_random_percent <= 95 THEN v_payment_method := 'GOOGLE_PAY'; v_payment_status := 'SUCCESS';
        ELSE v_payment_method := 'APPLE_PAY'; v_payment_status := 'SUCCESS'; END IF;

        IF NOT EXISTS (SELECT 1 FROM INVOICE WHERE TRACKING_NUMBER = rec.TRACKING_NUMBER) THEN
             INSERT INTO INVOICE (INVOICE_NUMBER, TRACKING_NUMBER, INVOICE_DATE, STATUS, TOTAL_PRICE)
             VALUES (v_invoice_number, rec.TRACKING_NUMBER, rec.SHIP_DATE, 'PAID', v_random_price);
             
             INSERT INTO PAYMENT (METHOD, INVOICE_NUMBER, PAYMENT_DATE, STATUS)
             VALUES (v_payment_method, v_invoice_number, rec.SHIP_DATE, v_payment_status);
        END IF;

        IF rec.STATUS NOT IN ('OUT_FOR_DELIVERY', 'DELIVERED', 'RETURNING', 'RETURNED', 'DELIVERY_FAILED') THEN
            CONTINUE;
        END IF;

        v_current_branch_id := rec.BRANCH_ID;
        v_is_delivery_failed := FALSE; 
        
        IF rec.STATUS = 'OUT_FOR_DELIVERY' THEN v_batch_date := NOW() - (floor(random()*3 + 1) * interval '1 hour'); 
        ELSE v_batch_date := rec.SHIP_DATE + interval '1 day' + (floor(random()*5) * interval '1 hour'); END IF;

        v_vehicle_id := (SELECT VEHICLE_ID FROM VEHICLE ORDER BY random() LIMIT 1);
        v_courier_id := (SELECT EMPLOYEE_ID FROM EMPLOYEE WHERE BRANCH_ID = v_current_branch_id AND TYPE='COURIER' ORDER BY random() LIMIT 1);
        IF v_courier_id IS NULL THEN v_courier_id := (SELECT EMPLOYEE_ID FROM COURIER LIMIT 1); END IF;

        INSERT INTO DELIVERY (ASSIGNED_AT, STATUS, COURIER_ID, VEHICLE_ID)
        VALUES (v_batch_date, 'IN_PROGRESS', v_courier_id, v_vehicle_id) RETURNING DELIVERY_ID INTO v_delivery_id;

        INSERT INTO HAS_PACKAGE (DELIVERY_ID, PACKAGE_BARCODE)
        SELECT v_delivery_id, PACKAGE_BARCODE FROM PACKAGE WHERE TRACKING_NUMBER = rec.TRACKING_NUMBER;

        INSERT INTO TRACKING_LOG (LOG_DATE, STATUS, BRANCH_ID, PACKAGE_BARCODE)
        SELECT v_batch_date, 'OUT_FOR_DELIVERY', rec.BRANCH_ID, PACKAGE_BARCODE FROM PACKAGE WHERE TRACKING_NUMBER = rec.TRACKING_NUMBER;

        IF rec.STATUS = 'OUT_FOR_DELIVERY' THEN UPDATE COURIER SET STATUS = 'BUSY' WHERE EMPLOYEE_ID = v_courier_id; CONTINUE; END IF; 

        v_delivery_duration := (floor(random() * 270) + 30) * interval '1 minute';
        v_log_date := v_batch_date + v_delivery_duration;

        IF rec.STATUS = 'DELIVERED' THEN
             UPDATE DELIVERY SET STATUS = 'COMPLETED', DELIVERED_AT = v_log_date WHERE DELIVERY_ID = v_delivery_id;

             INSERT INTO TRACKING_LOG (LOG_DATE, STATUS, BRANCH_ID, PACKAGE_BARCODE)
             SELECT v_log_date, 'DELIVERED', rec.BRANCH_ID, PACKAGE_BARCODE FROM PACKAGE WHERE TRACKING_NUMBER = rec.TRACKING_NUMBER;
             
             UPDATE PAYMENT P 
             SET STATUS = 'SUCCESS', PAYMENT_DATE = v_log_date
             FROM INVOICE I
             WHERE P.INVOICE_NUMBER = I.INVOICE_NUMBER 
             AND I.TRACKING_NUMBER = rec.TRACKING_NUMBER 
             AND P.METHOD = 'CASH';

        ELSIF rec.STATUS IN ('RETURNING', 'RETURNED', 'DELIVERY_FAILED') THEN
             UPDATE DELIVERY SET STATUS = 'FAILED' WHERE DELIVERY_ID = v_delivery_id;
             INSERT INTO TRACKING_LOG (LOG_DATE, STATUS, BRANCH_ID, PACKAGE_BARCODE)
             SELECT v_log_date, 'DELIVERY_FAILED', rec.BRANCH_ID, PACKAGE_BARCODE FROM PACKAGE WHERE TRACKING_NUMBER = rec.TRACKING_NUMBER;

             IF rec.STATUS != 'DELIVERY_FAILED' THEN
                 INSERT INTO TRACKING_LOG (LOG_DATE, STATUS, BRANCH_ID, PACKAGE_BARCODE)
                 SELECT v_log_date + interval '1 hour', 'RETURNING', rec.BRANCH_ID, PACKAGE_BARCODE FROM PACKAGE WHERE TRACKING_NUMBER = rec.TRACKING_NUMBER;
                 
                 IF rec.STATUS = 'RETURNING' THEN 
                    UPDATE INVOICE SET STATUS = 'ISSUED' WHERE TRACKING_NUMBER = rec.TRACKING_NUMBER;
                 ELSE
                    INSERT INTO TRACKING_LOG (LOG_DATE, STATUS, BRANCH_ID, PACKAGE_BARCODE)
                    SELECT v_log_date + interval '2 days', 'RETURNED', (SELECT BRANCH_ID FROM BRANCH ORDER BY random() LIMIT 1), PACKAGE_BARCODE FROM PACKAGE WHERE TRACKING_NUMBER = rec.TRACKING_NUMBER;
                    UPDATE INVOICE SET STATUS = 'CANCELLED' WHERE TRACKING_NUMBER = rec.TRACKING_NUMBER;
                    UPDATE PAYMENT SET STATUS = 'REFUNDED' WHERE INVOICE_NUMBER = (SELECT INVOICE_NUMBER FROM INVOICE WHERE TRACKING_NUMBER = rec.TRACKING_NUMBER) AND METHOD != 'CASH';
                 END IF;
             END IF;
        END IF;
    END LOOP;

    RAISE NOTICE 'Veri üretimi başarıyla tamamlandı!';
END $$;