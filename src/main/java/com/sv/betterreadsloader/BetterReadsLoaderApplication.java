package com.sv.betterreadsloader;

import com.sv.betterreadsloader.connection.DataStaxProperties;
import com.sv.betterreadsloader.domain.Author;
import com.sv.betterreadsloader.domain.Book;
import com.sv.betterreadsloader.repository.AuthorRepository;
import com.sv.betterreadsloader.repository.BookRepository;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.format.annotation.DateTimeFormat;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxProperties.class)
public class BetterReadsLoaderApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BetterReadsLoaderApplication.class, args);
    }


    private void initAuthors(){
        Path path = Paths.get(authorDumpLocation);
        try(Stream<String> lines =  Files.lines(path)){
            lines.forEach(line ->{
                try {
                    //Read and parse the line
                    String jsonString = line.substring(line.indexOf("{"));
                    JSONObject jsonObject = new JSONObject(jsonString);

                    //Create author object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));

                    //Persist
                    authorRepository.save(author);
                    System.out.println("author " + author.getName() +  " saved");
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private void initWorks(){
        Path path = Paths.get(worksDumpLocation);
        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try(Stream<String> lines =  Files.lines(path)){
            lines.forEach(line ->{
                try {
                    //Read and parse the line
                    String jsonString = line.substring(line.indexOf("{"));
                    JSONObject jsonObject = new JSONObject(jsonString);
                    var book = new Book();
                    book.setId(jsonObject.getString("key").replace("/works/",""));
                    book.setName(jsonObject.optString("title"));
                    var description = jsonObject.optJSONObject(jsonObject.optString("description"));
                    if (description != null){
                        book.setDescription(description.optString("description"));
                    }
                    var publishedDate = jsonObject.optJSONObject("created");
                    if (publishedDate != null){
                        book.setPublishedDate(LocalDate.parse(publishedDate.getString("value"), formatter));
                    }
                    var authorsJsonArray = jsonObject.optJSONArray("authors");
                    if (authorsJsonArray != null){
                        var authorIds = new ArrayList<String>();
                        for (int i=0;i<authorsJsonArray.length();i++){
                            var author = authorsJsonArray.getJSONObject(i).optJSONObject("author")
                                    .getString("key").
                                    replace("/authors/","");
                            authorIds.add(author);
                        }
                        book.setAuthorIds(authorIds);
                        var authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
                                .map(optionalAuthor -> {
                                    if (!optionalAuthor.isPresent())
                                        return "Unknown author";
                                    return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }
                    var coversJsonArray = jsonObject.optJSONArray("covers");
                    if (coversJsonArray != null){
                        var coverIds = new ArrayList<String>();
                        for (int i=0;i<coversJsonArray.length();i++){
                            coverIds.add(coversJsonArray.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }
                    bookRepository.save(book);
                    System.out.println("Record saved...");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void start(){
        System.out.println("Application started");
//        initAuthors();
        initWorks();
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxProperties dataStaxProperties){
        Path bundle = dataStaxProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

}
