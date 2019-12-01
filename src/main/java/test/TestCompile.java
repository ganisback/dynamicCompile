package test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;

import com.sun.tools.javac.Main;

public class TestCompile {

	private static String classPath = System.getProperty("user.dir");

	public static void main(String[] args) throws Exception {
		String expression = "a+b";
		// replace variable with real value
		expression = expression.replaceAll("a", "1").replaceAll("b", "2");
		double r = run(compile(expression));
		
		System.out.print(r);

	}

	private synchronized static File compile(String code) throws Exception {
		File file;
		// 创建一个临时java源文件
		file = File.createTempFile("JavaRuntime", ".java", new File(classPath));
		// 当Jvm 退出时 删除该文件
		//file.deleteOnExit();
		// 得到文件名和类名
		String filename = file.getName();
		String classname = getClassName(filename);
		// 将代码输出到源代码文件中
		PrintWriter out = new PrintWriter(new FileOutputStream(file));
		// 动态构造一个类,用于计算
		out.write("public class " + classname + "{" + "public static double main1(String[] args)" + "{");
		out.write("double result = " + code + ";");
		// 用于调试
		// out.write("System.out.println(result);");
		out.write("return new Double(result);");
		out.write("}}");
		// 关闭文件流
		out.flush();
		out.close();
		// 设置编译参数
		String[] args = new String[] { "-d", classPath+"/target/classes", filename };
		// 调试
		// Process process = Runtime.getRuntime().exec("javac " + filename);
		int status = Main.compile(args);
		// 输出运行的状态码.
		// 状态参数与对应值
		// EXIT_OK 0
		// EXIT_ERROR 1
		// EXIT_CMDERR 2
		// EXIT_SYSERR 3
		// EXIT_ABNORMAL 4
		// System.out.println(process.getOutputStream().toString());
		return file;
	}

	private static String getClassName(String filename) {
		return filename.substring(0, filename.length() - 5);
	}

	private synchronized static double run(File file) throws Exception {
		String filename = file.getName();
		String classname = getClassName(filename);
		Double tempResult = null;
		// System.out.println("class Name: " +classname);
		// 当Jvm 退出时候 删除生成的临时文件
		//new File(file.getParent(), classname + ".class").deleteOnExit();
		try {
			Class cls = Class.forName(classname);
			// System.out.println("run........");
			// 映射main1方法
			Method calculate = cls.getMethod("main1", new Class[] { String[].class });
			// 执行计算方法 得到计算的结果
			tempResult = (Double) calculate.invoke(null, new Object[] { new String[0] });
		} catch (SecurityException se) {
			System.out.println("something is wrong !!!!");
			System.out.println("请重新运行一遍");
		}
		// 返回值
		return tempResult.doubleValue();
	}

}
