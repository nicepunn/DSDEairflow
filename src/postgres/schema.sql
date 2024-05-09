CREATE TABLE "Article" (
  "id" int PRIMARY KEY,
  "title" varchar,
  "abstract" varchar,
  "publishdate" date
);

CREATE TABLE "ArticleKeywords" (
  "id" int PRIMARY KEY,
  "article_id" int,
  "keyword" varchar
);

ALTER TABLE "ArticleKeywords" ADD FOREIGN KEY ("article_id") REFERENCES "Article" ("id");
