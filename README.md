# 3F
Инструкция по установке и деплою:
1. Сбилдить проект командой: ./gradlew :shadowJar
2. Сконфигурировать config.txt в директории docker (указать корректные адреса mongo, zookeeper, учитывая, что mongo работает на локальной машине, а zookeeper в контейнере)
3. Запустить mongoDB (команда mongo) и hadoop (./sbin/start-all.sh)
4. Создать образ приложения:  
   4.1. в директории docker команда: docker build -t app  
   4.2. загрузить образ zookeeper
5. Запустить docker-compose из директории docker командой: docker-compose up 
