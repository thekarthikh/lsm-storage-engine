FROM maven:3.9.9-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -q package -DskipTests

FROM eclipse-temurin:11-jre-jammy
WORKDIR /app
COPY --from=build /build/target/benchmarks.jar /app/lsm-engine.jar
VOLUME ["/app/data"]
ENTRYPOINT ["java", "-cp", "/app/lsm-engine.jar", "com.lsm.demo.LsmDemo", "/app/data"]
