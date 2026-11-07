FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY java/pom.xml ./
COPY java/src ./src
RUN mvn -q package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/bbs-java-1.0.jar app.jar
CMD ["java", "-cp", "app.jar", "bbs.JavaServer"]
