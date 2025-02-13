-- Используем LAG, чтобы определить начало нового блока сообщений
-- (когда тип сообщения меняется)
WITH BlockStart AS (
    SELECT
        entity_id,
        type,
        created_by,
		-- приводим время к формату timestamp+03 (Московскому UTC+3)
        TO_TIMESTAMP(created_at) AS created_at,
        LAG(type) OVER (PARTITION BY entity_id ORDER BY created_at) AS prev_type
    FROM test.chat_messages
),
-- Оставляем только первые сообщения из каждого блока
FilteredMessages AS (
    SELECT
        entity_id,
        type,
        created_by,
        created_at
    FROM BlockStart
    WHERE
		-- null - первое сообщение в сделке
		prev_type IS NULL
		-- prev_type отличается от текущего type - исключаем очереди сообщений от одного источника
		OR prev_type != type
),
-- Выбираем только сообщения клиента (incoming_chat_message)
ClientMessages AS (
    SELECT
        entity_id,
        created_at AS client_message_time
    FROM FilteredMessages
    WHERE type = 'incoming_chat_message'
),
-- Выбираем только сообщения менеджера (outgoing_chat_message)
ManagerMessages AS (
    SELECT
        entity_id,
        created_by AS manager_id,
        created_at AS manager_message_time
    FROM FilteredMessages
    WHERE type = 'outgoing_chat_message'
),
-- Для каждого сообщения клиента находим ближайший ответ менеджера
ResponseTimes AS (
    SELECT
        cm.entity_id,
		mm.manager_id,
        cm.client_message_time,
		mm.manager_message_time AS manager_response_time,
		-- пронумеруем для каждого сообщения клиента в каждой сделке последующие ответы менеджера
		ROW_NUMBER() OVER (PARTITION BY cm.entity_id, cm.client_message_time ORDER BY mm.manager_message_time) AS rn
    FROM ClientMessages cm
    JOIN ManagerMessages mm ON cm.entity_id = mm.entity_id
	WHERE mm.manager_message_time > cm.client_message_time
),
-- Корректируем время ответа с учётом рабочего времени
AdjustedResponseTimes AS (
    SELECT
        entity_id,
        client_message_time,
		manager_id,
        manager_response_time,
        CASE
            -- Если ответ пришёл в рабочее время
            WHEN manager_response_time::time BETWEEN '09:30:00' AND '23:59:59'
                 AND client_message_time::time BETWEEN '09:30:00' AND '23:59:59'
			-- Считаем разницу в секундах
            THEN EXTRACT(EPOCH FROM (manager_response_time - client_message_time))
            -- Если клиент написал в нерабочее время, а ответ пришёл в рабочее
            WHEN client_message_time::time NOT BETWEEN '09:30:00' AND '23:59:59'
                 AND manager_response_time::time BETWEEN '09:30:00' AND '23:59:59'
			-- Приводим время сообщения клиента ко времени начала рабочего дня, и считаем разницу в секундах
            THEN EXTRACT(EPOCH FROM (manager_response_time
				- (DATE(client_message_time) + INTERVAL '9 hours 30 minutes')))
            -- Если клиент написал в рабочее время, а ответ пришёл в нерабочее
            WHEN client_message_time::time BETWEEN '09:30:00' AND '23:59:59'
                 AND manager_response_time::time NOT BETWEEN '09:30:00' AND '23:59:59'
			-- Приводим время ответа менеджера ко времени конца рабочего дня, и считаем разницу в секундах
            THEN EXTRACT(EPOCH FROM (DATE(manager_response_time) - client_message_time))
            -- Если и клиент, и ответ пришли в нерабочее время
            ELSE 0
			
        END AS adjusted_response_time
    FROM ResponseTimes
    WHERE
		rn = 1 AND							-- оставляем только первое сообщение менеджера
		manager_response_time IS NOT NULL	-- Исключаем случаи, когда ответа нет
)
SELECT
    m.name_mop AS manager_name,
    ROUND(AVG(art.adjusted_response_time)) AS avg_response_time_seconds,
	(ROUND(AVG(art.adjusted_response_time)) * INTERVAL '1 second')::time AS avg_response_time
FROM AdjustedResponseTimes art
JOIN test.managers m ON art.manager_id = m.mop_id
GROUP BY m.name_mop;
