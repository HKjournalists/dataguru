package work.RSA;

public class Test {
	public static void main(String[] args) {
		String plainTextString = "Hello World";
		
		KeyGenerater keyGenerater = new KeyGenerater();
		keyGenerater.generater();
		
		byte[] priKey = keyGenerater.getPriKey();
		byte[] pubKey = keyGenerater.getPubKey();
		
		byte[] signed = Signaturer.sign(priKey,plainTextString);
		
		boolean verify = SignProvider.verify(pubKey, plainTextString, signed);
		
		System.out.println("验证结果："+verify);
	}
}
