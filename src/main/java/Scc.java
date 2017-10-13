


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.Count;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 须小弥 on 2017/10/10.
 */
public class Scc {
    private static FileSystem fs;
    private static Configuration conf;
    public static void main(String args[]) throws IOException {
        conf=new Configuration();
        fs = FileSystem.get(conf);
        Scc scc=new Scc();
        //scc.mkdir("/data/scc1");
        //scc.put("d:\\fortest\\core-site.xml","/data/scc",false,false);
/*        List<String> ls=scc.ls("/data/scc",true);
        for (int i = 0; i <ls.size() ; i++) {
            System.out.println(ls.get(i));
        }*/

       /* List<LocatedFileStatus> ls=scc.tmpls("/data/scc",true);
        for (int i = 0; i <ls.size() ; i++) {
            System.out.println(ls.get(i));
        }*/
//        scc.count("/data/scc");

        //TODO 还需要继续修改啊
        //Path path=new Path("/data/scc");
        Path path=new Path("/data/scc");
        //scc.changeOwner(path);
        scc.changePermission(path,"744");
        //scc.createEmptyFile("/data/scc/word.txt");
//        scc.appendContentToFile("/data/scc/word.txt");
        //scc.PrintFileContent("/data/scc/word.txt");
    }

    /**
     * 创建指定路径下的空文件
     * @param fileName  文件路径（需要包含文件名）
     * @throws IOException
     */
    public void createEmptyFile(String fileName) throws IOException {
        Path path=new Path(fileName);
        fs.create(path);
    }

    /**
     * 查看文件内容
     * @param filePath  文件路径
     * @throws IOException
     */
    public void PrintFileContent(String filePath) throws IOException {
        Path path=new Path(filePath);
        FSDataInputStream fsDataInputStream=fs.open(path);
        InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream, "utf-8");
        BufferedReader bufferedReader=new BufferedReader(inputStreamReader);
        String str="";
        while ((str=bufferedReader.readLine())!=null){
            System.out.println(str);
        }
        bufferedReader.close();
        inputStreamReader.close();
        fsDataInputStream.close();
    }
    /**
     * 追加文件内容到指定文件
     * @param filePath  追加到某文件路径
     * @throws IOException
     */
    public void appendContentToFile(String filePath) throws IOException {
        Path path=new Path(filePath);
        FSDataOutputStream fsDataOutputStream=fs.append(path);//获取文件流
        Writer out=new OutputStreamWriter(fsDataOutputStream,"UTF-8");
        out.write("kkk");//填写内容到指定文件
        out.close();
    }
/*
   FSDataOutputStream fsDataOutputStream=
Writer out=new OutputStreamWriter(fsDataOutputStream);
        out.write();
        out.writeBytes(words);
        out.write(words.getBytes("UTF-8"));
        out.close();*/
    /**
     * 递归修改文件夹下所有文件的权限（包含子目录以及子目录下的文件）
     * @param path  文件（夹）路径
     * @param permissionCode
     */
    public void changePermission(Path path,String permissionCode) throws IOException {
        RemoteIterator<LocatedFileStatus> it=fs.listLocatedStatus(path);
        while (it.hasNext()){
            LocatedFileStatus locatedFileStatus=it.next();
            if (locatedFileStatus.isDirectory()) {
                fs.setPermission(locatedFileStatus.getPath(),new FsPermission(permissionCode));
                changePermission(locatedFileStatus.getPath(),permissionCode);
            }
            else{
                fs.setPermission(locatedFileStatus.getPath(),new FsPermission(permissionCode));
            }
        }
    }

    /**
     * 递归修改文件夹下的文件拥有者
     * @param path
     * @throws IOException
     */
    public void changeOwner(Path path) throws IOException {
        RemoteIterator<LocatedFileStatus> it=fs.listLocatedStatus(path);
        while (it.hasNext()){
            LocatedFileStatus locatedFileStatus=it.next();
            if (locatedFileStatus.isDirectory()) {
                fs.setOwner(locatedFileStatus.getPath(),"scc4944","supergroup");
                changeOwner(locatedFileStatus.getPath());
            }
            else{
                fs.setOwner(locatedFileStatus.getPath(),"scc4944","supergroup");
            }
        }
    }

    /**
     *  查看/data/姓名/目录下统计目录下文件数，空间占用情况。
     * @param src   路径
     * @throws IOException
     */
    public void count(String src) throws IOException {
        Path path=new Path(src);
        ContentSummary contentSummary=fs.getContentSummary(path);
        System.out.println(contentSummary);
    }

    /**
     * 创建文件目录
     * @param path 目录路径
     * @throws IOException
     */
    public void mkdir(String path) throws IOException {
        Path srcPath = new Path(path);
        fs.mkdirs(srcPath);
    }

    /**
     * 上传本地文件到HDFS
     * @param delSrc    是否删除源文件
     * @param overwrite 是否执行相同文件覆盖
     * @param src   源文件地址
     * @param dest  目标地址
     * @throws IOException
     */
    public void put(String src,String dest,boolean delSrc,boolean overwrite) throws IOException {
        Path srcPath=new Path(src);
        Path destPath=new Path(dest);
        fs.copyFromLocalFile(delSrc,overwrite,srcPath,destPath);
    }

    /**
     * 查看显示文件目录下的所有文件
     * @param src  目录路径
     * @param rec   是否递归显示
     */
    public List<String> ls(String src, boolean rec) throws IOException {
        List<String> listFiles=new ArrayList<String>();
        Path path=new Path(src);
        RemoteIterator<LocatedFileStatus> it=fs.listFiles(path,rec);
        //获取迭代器中的数据
        while (it.hasNext()){
            String filename=it.next().getPath().toString();
            listFiles.add(filename);
        }
        return listFiles;
    }

    public List<LocatedFileStatus> getFileStatus(String src, boolean rec) throws IOException {
        List<LocatedFileStatus> listFiles=new ArrayList<LocatedFileStatus>();
        Path path=new Path(src);
        RemoteIterator<LocatedFileStatus> it=fs.listFiles(path,rec);
        //获取迭代器中的数据
        while (it.hasNext()){
            //String filename=it.next().getPath().toString();
            listFiles.add(it.next());
        }
        return listFiles;
    }



}
