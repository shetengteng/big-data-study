package com.stt.demo.mr.Ch15_Compress;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Objects;

import static com.stt.demo.mr.Constant.INPUT;

public class TestCompress {

	public static void main(String[] args) throws Exception {

		//org.apache.hadoop.io.compress.DefaultCodec
		//org.apache.hadoop.io.compress.GzipCodec
		//org.apache.hadoop.io.compress.BZip2Codec
		compress(INPUT+"ch15/input.txt","org.apache.hadoop.io.compress.BZip2Codec");
		decompress(INPUT+"ch15/input.txt.bz2");
	}

	// 压缩
	private static void compress(String fileName,String className) throws Exception {

		FileInputStream fis = new FileInputStream(new File(fileName));

		CompressionCodec codec = (CompressionCodec) ReflectionUtils
				.newInstance(Class.forName(className),new Configuration());

		CompressionOutputStream cos = codec.createOutputStream(
				new FileOutputStream(
						new File(fileName+codec.getDefaultExtension())));
		// 流的对拷
		IOUtils.copyBytes(fis,cos,1024*1024,false);

		fis.close();
		cos.close();
	}

	// 解压
	private static void decompress(String fileName) throws Exception {
		// 校验是否可以解压缩
		CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
		CompressionCodec codec = factory.getCodec(new Path(fileName));

		if(Objects.isNull(codec)){
			System.out.println("cannot find codec for file:"+fileName);
			return;
		}
		// 获取输入流
		CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(fileName)));

		FileOutputStream fos = new FileOutputStream(new File(fileName+".decoded"));

		IOUtils.copyBytes(cis,fos,1024*1024,false);

		cis.close();
		fos.close();
	}
}