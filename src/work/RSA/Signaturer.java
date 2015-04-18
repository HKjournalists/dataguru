package work.RSA;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import com.gigya.socialize.Base64;

//使用私钥签名
public class Signaturer {
	 /**
	  * 
	  * Description:数字签名
	  * 
	  * @param priKeyText 私钥,base64编码
	  * @param plainText 明文
	  * @return
	  * @author 孙钰佳
	  * @since：2007-12-27 上午10:51:48
	  */
	 public static byte[] sign(byte[] priKeyText, String plainText) {
	  try {
		  PKCS8EncodedKeySpec priPKCS8 = new PKCS8EncodedKeySpec(Base64.decode(priKeyText));
		  KeyFactory keyf = KeyFactory.getInstance("RSA");
		  PrivateKey prikey = keyf.generatePrivate(priPKCS8);

		  // 用私钥对信息生成数字签名
		  java.security.Signature signet = java.security.Signature.getInstance("MD5withRSA");
		  signet.initSign(prikey);
		  signet.update(plainText.getBytes());
		  byte[] signed = Base64.encodeToByte(signet.sign(), false);
		  return signed;
	  } catch (java.lang.Exception e) {
		  System.out.println("签名失败");
		  e.printStackTrace();
	  }
	  return null;
	 }
}