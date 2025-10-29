Структура Data Lake
Основные директории:
/user/master/data/geo/events/date=YYYY-MM-DD/    # Сырые события соцсети с координатами, Партиционирование по дате

Рабочие и аналитические:
/user/kolaygrech/data/
│
├── geo/                      # Локальные справочники геоданных
│   |── geo.csv               # Список городов Австралии
│   |── geo_with_timezone.csv # Список городов Австралии с таймзонами
│   └── events/               	# ODD: очищенные и обогащённые события (все типы)
│       └── date=YYYY-MM-DD/  	# партиционирование по дате
│
├── analytics/                # Data Sandbox: витрины для анализа
│   ├── user_activity/        # актуальный и домашний город, поездки
│   ├── geo_zones/            # статистика по городам
│   |── user_last_city/       # вспомогательная витрина с актуальным городом пользователя
│   |── events_closest_city/  # вспомогательная витрина событий(сообщений) с городом в котором оно произошло
│   |── events_geo_full/      # вспомогательная витрина - все события с актуальным городом
│   └── friend_recommend/     # рекомендации друзей
│
└── tmp/                    # временные данные

Обновление и форматы
Слой	                    	                Частота	    					                            Формат
/kolaygrech/data/geo/events/                    ежедневно                                                   Parquet
/kolaygrech/data/geo/	                        по мере обновления справочника	                            CSV
/kolaygrech/data/analytics/	                    ежедневно (через Airflow DAG)	                            Parquet
/kolaygrech/data/analytics/geo_zones/           для снижения нагрузки можно не жедневно, а еженедельно      Parquet