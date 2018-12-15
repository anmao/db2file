import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


public class db2file {
    private static final String PROPERTIES_PATH = "/p.properties";
    private static final String JDBC_DRIVER_8 = "com.mysql.cj.jdbc.Driver";
    private static final String SUFFIX = ".txt";


    private static String[] getDate() {
        Calendar date = Calendar.getInstance();
        int month = date.get(Calendar.MONTH) + 1;
        String preDate = date.get(Calendar.YEAR)
                + "-" + month
                + "-" + date.get(Calendar.DAY_OF_MONTH)
                + " 00:00:00";

        date.add(Calendar.DATE, 1);
        month = date.get(Calendar.MONTH) + 1;
        String folDate = date.get(Calendar.YEAR)
                + "-" + month
                + "-" + date.get(Calendar.DAY_OF_MONTH)
                + " 00:00:00";
        String[] resultSet = {preDate, folDate};
        return resultSet;
    }


    private static String[] getDate(String assignDate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Date dateSDF = sdf.parse(assignDate);
        Calendar date = Calendar.getInstance();
        date.setTime(dateSDF);
        int month = date.get(Calendar.MONTH) + 1;
        String preDate = date.get(Calendar.YEAR)
                + "-" + month
                + "-" + date.get(Calendar.DAY_OF_MONTH)
                + " 00:00:00";

        date.add(Calendar.DATE, 1);
        month = date.get(Calendar.MONTH) + 1;
        String folDate = date.get(Calendar.YEAR)
                + "-" + month
                + "-" + date.get(Calendar.DAY_OF_MONTH)
                + " 00:00:00";
        String[] resultSet = {preDate, folDate};
        return resultSet;
    }


    private static String[] getDate(String[] dateInterval) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String preDateStr = dateInterval[0];
        Date dateSDF = sdf.parse(preDateStr);
        Calendar date = Calendar.getInstance();
        date.setTime(dateSDF);
        int month = date.get(Calendar.MONTH) + 1;
        String preDate = date.get(Calendar.YEAR)
                + "-" + month
                + "-" + date.get(Calendar.DAY_OF_MONTH)
                + " 00:00:00";

        String folDateStr = dateInterval[1];
        dateSDF = sdf.parse(folDateStr);
        date.setTime(dateSDF);
        month = date.get(Calendar.MONTH) + 1;
        String folDate = date.get(Calendar.YEAR)
                + "-" + month
                + "-" + date.get(Calendar.DAY_OF_MONTH)
                + " 00:00:00";
        String[] resultSet = {preDate, folDate};
        return resultSet;
    }


    // 生成sql语句
    private static String[] generateSQL(String[] storeMode,
                                        String[] tableNames,
                                        String script,
                                        String nullMode,
                                        Map<String, String[]> tablesCName,
                                        String fieldNames) throws ParseException {
        String[] resultSQLSet;
        int tableNumber = tableNames.length;

        String selectedField = "";
        if (!fieldNames.equals("")) {
            for (String field : fieldNames.split(",")) {
                selectedField += field + ",";
            }
        }
        selectedField = selectedField.substring(0, selectedField.length() - 1);

        if (storeMode[0].equals("manual")) {
            resultSQLSet = new String[1];
            resultSQLSet[0] = script;
        } else {
            resultSQLSet = new String[tableNumber];
            for (int i = 0; i < tableNumber; i++) {
                String tableName = tableNames[i];
                String resultSQL;
                if (selectedField.equals(""))
                    resultSQL = "select * from " + tableName + " where adddatetime between '";
                else
                    resultSQL = "select " + selectedField + " from " + tableName + " where adddatetime between '";

                // 检查查询模式
                int modeLength = storeMode.length;
                if (modeLength < 2) {  // 非指定日期区间
                    String mode = storeMode[0];
                    switch (mode) {
                        case "all": {  // 查询全部
                            if (selectedField.equals(""))
                                resultSQL = "select * from " + tableName;
                            else
                                resultSQL = "select " + selectedField + " from " + tableName;
                            if (nullMode.equals("n") || nullMode.equals("no")) {
                                resultSQL += " where";
                                for (String cName : tablesCName.get(tableName)) {
                                    resultSQL += " " + cName + " is not null and";
                                }
                                resultSQL = resultSQL.substring(0, resultSQL.length() - 4);
                            }
                            break;
                        }
                        case "today": {  // 查询当天
                            String[] dateStrings = getDate();
                            resultSQL += dateStrings[0]
                                    + "' and '"
                                    + dateStrings[1]
                                    + "'";
                            break;
                        }
                        default: {  // 指定单日日期
                            String assignDate = mode;
                            String[] dateStrings = getDate(assignDate);
                            resultSQL += dateStrings[0]
                                    + "' and '"
                                    + dateStrings[1]
                                    + "'";
                        }
                    }

                } else {  // 指定日期区间
                    String[] dateStrings = getDate(storeMode);
                    resultSQL = resultSQL
                            + dateStrings[0]
                            + "' and '"
                            + dateStrings[1]
                            + "'";
                }

                // 空值检测
                if (!storeMode[0].equals("all") && (nullMode.equals("n") || nullMode.equals("no"))) {
                    for (String cName : tablesCName.get(tableName)) {
                        resultSQL += " and " + cName + " is not null";
                    }
                }
                resultSQL += ";";

                resultSQLSet[i] = resultSQL;
            }

        }
        return resultSQLSet;
    }


    private static void downloadDBData(String[] SQLSet,
                                       String[] tablesName,
                                       Map<String, String[]> tablesCName,
                                       String outputPath,
                                       String separator,
                                       String[] storeMode,
                                       Logger logger,
                                       Connection conn,
                                       Statement stmt,
                                       boolean overrideFlag,
                                       String fieldNames) throws IOException, SQLException {
        // 生成需要存储的字段名
        String[] selectedNames = null;
        if (!fieldNames.equals(""))
            selectedNames = fieldNames.split(",");

        // 得到当天日期和模式，用于生成文件名
        Calendar todayDate = Calendar.getInstance();
        int todayMonth = todayDate.get(Calendar.MONTH) + 1;
        String dateStr = todayDate.get(Calendar.YEAR)
                + "-" + todayMonth
                + "-" + todayDate.get(Calendar.DAY_OF_MONTH);
        StringBuilder modeStr = new StringBuilder();
        for (int i = 0; i < storeMode.length; i++) {
            if (i != 0) {
                modeStr.append("-");
            }
            modeStr.append(storeMode[i]);
        }
        String modeFilePart = modeStr.toString();

        // 执行每条SQL语句
        ResultSet rs;
        for (int index = 0; index < SQLSet.length; index++){
            // 获取当前处理表名，字段名
            String tableName = tablesName[index];
            String[] tableCName = tablesCName.get(tableName);
            String sql = SQLSet[index];

            // 创建输出文件
            String outputFilePath = outputPath + "/" + tableName + "_" + modeFilePart +"_" + dateStr + SUFFIX;
            FileWriter writer;
            try {
                if (!overrideFlag) {
                    writer = new FileWriter(outputFilePath, true);
                } else {
                    writer = new FileWriter(outputFilePath, false);
                }
            } catch (IOException ioe) {
                logger.fatal("FailCreateOutputFile_" + tableName + ".");
                ioe.printStackTrace();
                throw ioe;
            }
            logger.info("SuccessFileCreate_" + tableName + ".");

            // 执行sql
            try {
                rs = stmt.executeQuery(sql);
            } catch (SQLException sqle) {
                logger.fatal("FailExecuteSQL_" + tableName + ".");
                sqle.printStackTrace();
                try {
                    writer.close();
                } catch (Exception ignore) {}
                throw sqle;
            }
            logger.info("SuccessExecuteSQL_" + tableName + ".");

            //输出到文件
            try {
                int count = 0;
                while (rs.next()) {
                    String singleResult = "";
                    int cNameLength;
                    if (fieldNames.equals(""))
                        cNameLength = tableCName.length;
                    else
                        cNameLength = selectedNames.length;
                    for (int i = 1; i <= cNameLength; i++) {
                        String queryC = rs.getString(i);
                        singleResult += queryC + separator;
                    }
                    singleResult = singleResult.substring(0, singleResult.length() - 1) + "\n";

                    writer.write(singleResult);
                    writer.flush();
                    count++;
                    logger.info("SuccessWriteFile_" + tableName + "_" + count + ".");
                }
                writer.close();
            } catch (SQLException sqle) {
                logger.fatal("FailQuerySQL_" + tableName + ".");
                sqle.printStackTrace();
                try {
                    writer.close();
                    rs.close();
                } catch (Exception ignore) {}
                throw sqle;
            } catch (IOException ioe) {
                logger.fatal("");
                ioe.printStackTrace();
                try {
                    writer.close();
                    rs.close();
                } catch (Exception ignore) {}
                throw ioe;
            }

            // 清理
            try {
                rs.close();
            } catch (SQLException sqle) {
                logger.fatal("FailCloseRS.");
                sqle.printStackTrace();
                throw sqle;
            }
            logger.info("SuccessQuerySQL_" + tableName + ".");
        }
    }


    private static void downloadDBData(String[] SQLSet,
                                       String[] tablesName,
                                       Map<String, String[]> tablesCName,
                                       String outputPath,
                                       String separator,
                                       String[] storeMode,
                                       Logger logger,
                                       Connection conn,
                                       Statement stmt,
                                       long lastStringIndex,
                                       String fieldNames) throws IOException, SQLException {
        // 生成需要存储的字段名
        String[] selectedNames = null;
        if (!fieldNames.equals(""))
            selectedNames = fieldNames.split(",");

        // 得到当天日期和模式，用于生成文件名
        Calendar todayDate = Calendar.getInstance();
        int todayMonth = todayDate.get(Calendar.MONTH) + 1;
        String dateStr = todayDate.get(Calendar.YEAR)
                + "-" + todayMonth
                + "-" + todayDate.get(Calendar.DAY_OF_MONTH);
        StringBuilder modeStr = new StringBuilder();
        for (int i = 0; i < storeMode.length; i++) {
            if (i != 0) {
                modeStr.append("-");
            }
            modeStr.append(storeMode[i]);
        }
        String modeFilePart = modeStr.toString();

        // 执行每条SQL语句
        ResultSet rs;
        boolean firstSQL = true;
        for (int index = 0; index < SQLSet.length; index++){
            // 判断是不是第一条语句，需要从新执行
            if (index != 0) {
                firstSQL = false;
            }

            // 获取当前处理表名，字段名
            String tableName = tablesName[index];
            String[] tableCName = tablesCName.get(tableName);
            String sql = SQLSet[index];

            // 创建输出文件
            String outputFilePath = outputPath + "/" + tableName + "_" + modeFilePart +"_" + dateStr + SUFFIX;
            FileWriter writer;
            try {
                writer = new FileWriter(outputFilePath, true);
            } catch (IOException ioe) {
                logger.fatal("FailCreateOutputFile_" + tableName + ".");
                ioe.printStackTrace();
                throw ioe;
            }
            logger.info("SuccessFileCreate_" + tableName + ".");

            // 执行sql
            try {
                rs = stmt.executeQuery(sql);
            } catch (SQLException sqle) {
                logger.fatal("FailExecuteSQL_" + tableName + ".");
                sqle.printStackTrace();
                try {
                    writer.close();
                } catch (Exception ignore) {}
                throw sqle;
            }
            logger.info("SuccessExecuteSQL_" + tableName + ".");

            //输出到文件
            try {
                int count = 0;
                while (rs.next()) {
                    // 找到最后一次成功的位置
                    if (firstSQL && count < lastStringIndex) {
                        count++;
                        continue;
                    }

                    String singleResult = "";
                    int cNameLength;
                    if (fieldNames.equals(""))
                        cNameLength = tableCName.length;
                    else
                        cNameLength = selectedNames.length;

                    for (int i = 1; i <= cNameLength; i++) {
                        String queryC = rs.getString(i);
                        singleResult += queryC + separator;
                    }
                    singleResult = singleResult.substring(0, singleResult.length() - 1) + "\n";

                    writer.write(singleResult);
                    writer.flush();
                    count++;
                    logger.info("SuccessWriteFile_" + tableName + "_" + count + ".");
                }
                writer.close();
            } catch (SQLException sqle) {
                logger.fatal("FailQuerySQL_" + tableName + ".");
                sqle.printStackTrace();
                try {
                    writer.close();
                    rs.close();
                } catch (Exception ignore) {}
                throw sqle;
            } catch (IOException ioe) {
                logger.fatal("");
                ioe.printStackTrace();
                try {
                    writer.close();
                    rs.close();
                } catch (Exception ignore) {}
                throw ioe;
            }

            // 清理
            try {
                rs.close();
            } catch (SQLException sqle) {
                logger.fatal("FailCloseRS.");
                sqle.printStackTrace();
                throw sqle;
            }
            logger.info("SuccessQuerySQL_" + tableName + ".");
        }
    }


    private static String readLastNLine(String filePath, long numRead)
    {
        File file = new File(filePath);

        //行数统计
        long count = 0;

        // 排除不可读状态
        if (!file.exists() || file.isDirectory() || !file.canRead())
        {
            return null;
        }

        // 使用随机读取，读模式
        try (RandomAccessFile fileRead = new RandomAccessFile(file, "r")) {
            //读取文件长度
            long length = fileRead.length();

            //如果是0，代表是空文件，直接返回空结果
            if (length == 0L) {
                return "";
            } else {
                //初始化游标
                long pos = length - 1;
                while (pos > 0) {
                    pos--;
                    //开始读取
                    fileRead.seek(pos);
                    //如果读取到\n代表是读取到一行
                    if (fileRead.readByte() == '\n') {
                        //使用readLine获取当前行
                        String line = fileRead.readLine();

                        //行数统计，如果到达了numRead指定的行数，就跳出循环
                        count++;
                        if (count == numRead) {
                            return line;
                        }
                    }
                }
                if (pos == 0) {
                    fileRead.seek(0);
                    return fileRead.readLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }


    public static void main(String[] args) {
        // 读取XML文件，得到dailyLog文件地址
        File xmlFile = new File("./log4j2.xml");
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        String logHome = "";
        try {
            DocumentBuilder builder = builderFactory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName("property");
            logHome = nList.item(0).getTextContent();
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }

        // 初始化日志文件
        String log4jPath = System.getProperty("user.dir") + "/log4j2.xml";
        ConfigurationSource source = null;
        try {
            source = new ConfigurationSource(new FileInputStream(log4jPath));
        } catch (IOException io) {
            io.printStackTrace();
            System.out.println("CanNotFind log4j2.xml");
            return;
        }
        Configurator.initialize(null, source);
        Logger logger = LogManager.getLogger("dailyLogger");

        // 检验是否出现中断
        // 若上次正常完成读取，则状态0
        // 若上次出现某错误，则状态1
        // 若上次只完成某表正常读取，则状态2
        // 若上次只完成某表中某行正确读取，则状态3
        String lastLog = readLastNLine(logHome + "/dailyLog.log", 1);  // 暂时的路径 jar包中要改
        String logStatus = null;
        int logStatuFlag = -1;
        String lastTableName = "";
        long lastStringIndex = -1;
        if (lastLog == null) {
            System.out.println("FailReadLogFile.");
            return;
        } else if (lastLog.equals("")) {
            logStatuFlag = 0;
        } else {
            logStatus = lastLog.split(" ")[6];
            if (logStatus.equals("SuccessAll.") || logStatus.equals("")) {
                logStatuFlag = 0;
            } else {
                String[] statusInfo = logStatus.split("_");

                // 只完成某表正常读取
                if (statusInfo.length == 1) {
                    logStatuFlag = 1;
                }
                if (statusInfo.length == 2) {
                    logStatuFlag = 2;
                    lastTableName = statusInfo[1];
                }
                if (statusInfo.length == 3) {
                    logStatuFlag = 3;
                    lastTableName = statusInfo[1];
                    lastStringIndex = Long.parseLong(statusInfo[2].substring(0, statusInfo[2].length() - 1));
                }
            }
        }


        // 读取配置文件
        Properties props = new Properties();
        String filePath = System.getProperty("user.dir") + PROPERTIES_PATH;
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(filePath));
            props.load(in);
        } catch (IOException io) {
            logger.fatal("PropertiesFileNotFound.");
            io.printStackTrace();
            return;
        }
        String dbIP = props.getProperty("ip");  // 获取ip
        String dbPort = props.getProperty("port");  // 获取port
        String dbName = props.getProperty("db_name");  // 获取数据库名
        String dbUserId = props.getProperty("user");  // 获取用户名
        String dbUserPw = props.getProperty("password");  // 获取密码
        String[] tablesName = props.getProperty("tables_name").split(",");  // 获取待处理表名
        String outputPath = props.getProperty("output_path");  // 获取输出目录
        String nullMode = props.getProperty("store_null_data");  // 空值存储模式
        String[] storeMode = props.getProperty("get_way").split(",");  // 数据查询模式
        String separator = props.getProperty("separator");  // 获取数据分隔符
        String script = props.getProperty("script");  // "manual"模式下的查询语句
        String fieldNames = props.getProperty("field_names");
        switch (separator) {
            case "\\b":
            case "space": {
                separator = "\b";
                break;
            }
            case "\\t":
            case "tab": {
                separator = "\t";
                break;
            }
            case ",":
            case "comma":
            default: {
                separator = ",";
                break;
            }
        }

        // 生成sql连接语句
        String dbUrl = "jdbc:mysql://" + dbIP +
                ":" + dbPort +
                "/" + dbName +
                "?" + "useUnicode=true" +
                "&" + "characterEncoding=utf-8" +
                "&" + "autoReconnect=true" +
                "&" + "failOverReadOnly=false" +
                "&" + "maxReconnects=10";

        // 尝试连接sql服务器
        Connection conn;
        Statement stmt = null;

        try {
            Class.forName(JDBC_DRIVER_8);
            conn = DriverManager.getConnection(dbUrl, dbUserId, dbUserPw);
            stmt = conn.createStatement();
        } catch (ClassNotFoundException | SQLException e){
            logger.fatal("FailInitJDBC.");
            e.printStackTrace();
            return;
        }
        logger.info("SuccessServerConnection.");

        // 获取每个表的字段名
        Map<String, String[]> tablesCName = new HashMap<String, String[]>();
        try {
            String preCNameSQL = "select COLUMN_NAME from information_schema.COLUMNS where table_name = ";
            for (String tableName : tablesName) {
                String getCNameSQL = preCNameSQL + "'" + tableName + "';";
                ResultSet rsCName = stmt.executeQuery(getCNameSQL);

                rsCName.last();
                int rsLength = rsCName.getRow();
                String[] tempResult = new String[rsLength];
                rsCName.beforeFirst();


                int index = 0;
                while (rsCName.next()) {
                    tempResult[index] = rsCName.getString("COLUMN_NAME");
                    index++;
                }
                tablesCName.put(tableName, tempResult);
                rsCName.close();
            }
        } catch (SQLException se) {
            logger.fatal("FailGetTablesCName.");
            se.printStackTrace();
            try {
                stmt.close();
                conn.close();
            } catch (SQLException ignore) {}
            return;
        }

        // 生成SQL语句序列
        String[] SQLSet;
        try {
            SQLSet = generateSQL(storeMode,
                    tablesName,
                    script,
                    nullMode,
                    tablesCName,
                    fieldNames);
        } catch (ParseException p) {
            logger.fatal("FailParseDate.");
            p.printStackTrace();
            try {
                stmt.close();
                conn.close();
            } catch (SQLException ignore) {}
            return;
        }
        logger.info("SuccessSqlGenerate.");

        // 执行生成的SQL语句并存放到本地
        try {
            switch (logStatuFlag) {
                // 上次成功
                case 0: {
                    downloadDBData(SQLSet, tablesName, tablesCName, outputPath, separator,
                            storeMode, logger, conn, stmt, false, fieldNames);
                    break;
                }
                // 上次没写则失败
                case 1: {
                    downloadDBData(SQLSet, tablesName, tablesCName, outputPath, separator,
                            storeMode, logger, conn, stmt, true, fieldNames);
                    break;
                }
                // 上次成功写到某表
                case 2: {
                    int lastTablePos = 0;
                    lastStringIndex = 0;
                    lastTableName = lastTableName.substring(0, lastTableName.length() - 1);
                    while (!SQLSet[lastTablePos].split(" ")[3].equals(lastTableName)) {lastTablePos++;}
                    String[] reExeSqlSet = new String[SQLSet.length - lastTablePos - 1];
                    String[] reTablesName = new String[SQLSet.length - lastTablePos - 1];
                    lastTablePos++;
                    int index = 0;
                    while (lastTablePos < SQLSet.length) {
                        reExeSqlSet[index] = SQLSet[lastTablePos];
                        reTablesName[index] = SQLSet[lastTablePos].split(" ")[3];
                        index++;
                        lastTablePos++;
                    }
                    downloadDBData(reExeSqlSet, reTablesName, tablesCName, outputPath, separator, storeMode, logger,
                            conn, stmt, lastStringIndex, fieldNames);
                    break;
                }
                // 上次成功写到某行
                case 3: {
                    int lastTablePos = 0;
                    while (!SQLSet[lastTablePos].split(" ")[3].equals(lastTableName)) {lastTablePos++;}
                    String[] reExeSqlSet = new String[SQLSet.length - lastTablePos];
                    String[] reTablesName = new String[SQLSet.length - lastTablePos];
                    int index = 0;
                    while (lastTablePos < SQLSet.length) {
                        reExeSqlSet[index] = SQLSet[lastTablePos];
                        reTablesName[index] = SQLSet[lastTablePos].split(" ")[3];
                        index++;
                        lastTablePos++;
                    }
                    downloadDBData(reExeSqlSet, reTablesName, tablesCName, outputPath, separator, storeMode, logger,
                            conn, stmt, lastStringIndex, fieldNames);
                    break;
                }
            }
        } catch (SQLException | IOException e) {
            try {
                stmt.close();
                conn.close();
            } catch (SQLException ignore) {
            }
            return;
        }
        // 清理
        try {
            stmt.close();
            conn.close();
        } catch (SQLException ignored) {}
        logger.info("SuccessAll.");
    }
}