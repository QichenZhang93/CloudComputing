

import java.math.BigInteger;

public class Decode {
	private final static String X = "26362928081002303087713271852220986810381868672656104434480496829300700800354435353790711445136516282";
    
    public static String decode(String Y, String C) {
        
        // KeyGen
        int K = KeyGen(Y);
        
        // Spiralize
        String I = Spiralize(C);
        
        // Caesarify
        String output = Caesarify(K, I);
        
        return output;
    }

    private static int KeyGen(String Y) {
        int yLength = Y.length();
        int xLength = X.length();
        int cursor = 0;
        while (cursor<=xLength-yLength) {
            StringBuilder zBuilder = new StringBuilder();
            for (int i = 0; i < yLength; i++) {
                //TODO: new version
                int x_char = X.charAt(cursor+i) - '0';
                int y_char = Y.charAt(i) - '0';
                zBuilder.append((x_char+y_char) % 10);

            }
            Y = zBuilder.toString();
            cursor++;
        }
        BigInteger bigInteger = new BigInteger(Y);
        int K = bigInteger.divideAndRemainder(new BigInteger("25"))[1].intValue() + 1;
        return K;
    }
    
    private static String Caesarify(int K, String I) {
        StringBuilder output = new StringBuilder();
        for (int j = 0; j < I.length(); j++) {
            int ASC = I.charAt(j)-K;
            if (ASC < 'A') {
                output.append((char)('Z'-'A'+ASC+1));
            }
            else {
                output.append((char)(ASC));
            }
        }
        return output.toString();
    }
    
    private static String Spiralize(String C) {
        int cLength = C.length();
        int height;
        for (height=0; height<cLength; height++) {
            if (height*(height+1)/2==cLength) {
                break;
            }
        }
        StringBuilder builder = new StringBuilder();
        
        int layer = 0;
        while (height > 0) {
            int head = 2*layer*(layer+1);
            int step = 0;
            for (int i = 0; i < height; i++) {
                step = head+i*(1+layer*2)+i*(i-1)/2;
                builder.append(C.charAt(step));
            }
            for (int i = 0; i < height-1; i++) {
                step++;
                builder.append(C.charAt(step));
            }
            step = 0;
            for (int i = height-2; i > 0; i--) {
                step = head+i*(2+layer*2)+i*(i-1)/2;
                builder.append(C.charAt(step));
            }
            layer++;
            height -= 3;
        }
        
        return builder.toString();
    }
}