package ru.erik182;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class Application {

    private static final String PROPERTIES_URL = "src/main/resources/application.properties";
    private static Properties properties;

    private static AmazonS3 s3;

    private static final String BUCKET_LIST_COMMAND = "list";
    private static final String BUCKET_DOWNLOAD_COMMAND = "download";
    private static final String BUCKET_UPLOAD_COMMAND = "upload";
    private static final String BUCKET_ALBUM_PROPERTY = "-a";
    private static final String BUCKET_PATH_PROPERTY = "-p";

    private static final String PHOTO_SUFFIX_1 = ".jpg";
    private static final String PHOTO_SUFFIX_2 = ".jpg";

    // инициализация
    static {
        FileInputStream fis;
        properties = new Properties();

        try {
            fis = new FileInputStream(PROPERTIES_URL);
            properties.load(fis);

            AWSCredentials credentials = new BasicAWSCredentials(
                    properties.getProperty("aws.accesskey"),
                    properties.getProperty("aws.secretkey")
            );
            s3 = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(Regions.AP_SOUTH_1)
                    .build();

        } catch (Exception e) {
            System.err.println("An error occurred while initializing the application." + e.getMessage());
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        try {
            List<String> argsList = Arrays.asList(args);
            if(argsList.size() == 0) throw new IllegalArgumentException();

            switch (argsList.get(0)) {

                // list
                case BUCKET_LIST_COMMAND: {
                    // list of buckets
                    if(argsList.size() == 1){
                        System.out.println(s3.listBuckets().stream().map(Bucket::getName).collect(Collectors.toList()));
                    }
                    // list of files in bucket
                    else {
                        String param = argsList.get(1);
                        String albumName = argsList.get(2);
                        if(!(argsList.size() == 3 && param.equals(BUCKET_ALBUM_PROPERTY)))
                            throw new IllegalArgumentException("Parameters should be like: list -a *album_name*");
                        try {
                            ObjectListing objectListing = s3.listObjects(albumName);
                            for(S3ObjectSummary os : objectListing.getObjectSummaries()) {
                                String name = os.getKey();
                                if(name.endsWith(PHOTO_SUFFIX_1) || name.endsWith(PHOTO_SUFFIX_2))
                                    System.out.println(name);
                            }
                        }
                        catch (AmazonS3Exception e){
                            System.err.println("The specified album does not exist. " + e.getMessage());
                        }
                    }
                } break;

                // download
                case BUCKET_DOWNLOAD_COMMAND: {
                    if(
                            argsList.size() != 5 ||
                            !argsList.get(1).equals(BUCKET_PATH_PROPERTY) ||
                            !argsList.get(3).equals(BUCKET_ALBUM_PROPERTY)
                    )
                        throw new IllegalArgumentException("Parameters should be like: download -p *path* -a *album_name*");
                    String path = argsList.get(2);
                    String albumName = argsList.get(4);
                    try {
                        ObjectListing objectListing = s3.listObjects(albumName);
                        File directory = new File(path);
                        if(!(directory.isDirectory() && directory.exists()))
                            throw new IllegalArgumentException("Invalid path specified *path*");
                        for(S3ObjectSummary os : objectListing.getObjectSummaries()) {
                            String name = os.getKey();
                            if(name.endsWith(PHOTO_SUFFIX_1) || name.endsWith(PHOTO_SUFFIX_2)){
                                S3Object s3object = s3.getObject(albumName, name);
                                S3ObjectInputStream inputStream = s3object.getObjectContent();
                                FileUtils.copyInputStreamToFile(inputStream, new File(path + "/" + name));
                            }
                        }
                    }
                    catch (AmazonS3Exception e){
                        System.err.println("The specified album does not exist. " + e.getMessage());
                    }
                } break;

                // upload
                case BUCKET_UPLOAD_COMMAND:{
                    if(
                            argsList.size() != 5 ||
                                    !argsList.get(1).equals(BUCKET_PATH_PROPERTY) ||
                                    !argsList.get(3).equals(BUCKET_ALBUM_PROPERTY)
                    )
                        throw new IllegalArgumentException("Parameters should be like: upload -p *path* -a *album_name*");
                    String path = argsList.get(2);
                    String albumName = argsList.get(4);

                    File directory = new File(path);
                    if(!(directory.isDirectory() && directory.exists()))
                        throw new IllegalArgumentException("Invalid path specified *path*");
                    if(!s3.doesBucketExist(albumName)) {
                        s3.createBucket(albumName);
                    }

                    for(File file: Objects.requireNonNull(directory.listFiles())){
                        if(!file.isDirectory() && (file.getName().endsWith(PHOTO_SUFFIX_1) || file.getName().endsWith(PHOTO_SUFFIX_2)) ) {
                            s3.putObject(
                                    albumName,
                                    file.getName(),
                                    file
                            );
                        }
                    }

                } break;

                default:{
                    System.err.println("Launch parameters error ");
                }
            }
        }
        catch (IllegalArgumentException e){
            System.err.println("Launch parameters error: " + e.getLocalizedMessage());
        }
        catch (Exception e){
            System.err.println("Error: " + e.getLocalizedMessage());
        }
    }
}
