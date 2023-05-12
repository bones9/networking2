import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class testingthings {


    //main method
    public static void main(String [] args){

        String message1 = "REBALANCE 0 0";
        String message2 = "REBALANCE 1 goodbye.txt 3 6005 6004 6002 1 3rd.txt";
        String message3 = "REBALANCE 2 heyy.csv 1 3001 goodbye.txt 2 6004 6002 1 3rd.txt";
        String message4 = "REBALANCE 2 heyy.csv 1 3001 goodbye.txt 2 6004 6002 3 3rd.txt yooo.hs bones.db";
        String message5 = "REBALANCE 4 lol.bob 1 300 koko 3 89 90 191 heyy.csv 1 3001 goodbye.txt 2 6004 6002 0";
        String message6 = "REBALANCE 4 lol.bob 1 300 koko 3 89 90 191 heyy.csv 1 3001 goodbye.txt 2 6004 6002 2 ah oh";

        decodeMessage(message1);
        System.out.println(" |XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX|");
        decodeMessage(message2);
        System.out.println(" |XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX|");
        decodeMessage(message3);
        System.out.println(" |XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX|");
        decodeMessage(message4);
        System.out.println(" |XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX|");
        decodeMessage(message5);
        System.out.println(" |XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX|");
        decodeMessage(message6);


    }




    public static void decodeMessage(String message){

        // Initialize data structures to store the files to send and delete
        Map<String, List<Integer>> filesToSend = new HashMap<>();
        List<String> filesToDelete = new ArrayList<>();



        System.out.println("message issssss:: " + message);
        if (message.replaceAll(" ", "").equals("REBALANCE00")){
            //  logger.info("Files to send: " + filesToSend);
            // logger.info("Files to delete: " + filesToDelete);
            System.out.println("message was REBALANCE 0 0 so no files to send or delete");
            //send the controller a REBALANCE_COMPLETE message
            return;
        }
        System.out.println("we never did  the if statement");




        String[] output = new String[2];

        //split the message into individual parts by spaces
        String[] parts = message.split(" ");
        //reverse the order of the parts array
        String[] reversedParts = new String[parts.length];
        for (int i = 0; i < parts.length; i++){
            reversedParts[i] = parts[parts.length - 1 - i];
        }
        //System.out.println("reversedParts is: " + Arrays.toString(reversedParts));

        //find the index of the first element in the reversedParts array that is a number
        int firstNumberIndex = 0;
        for (int i = 0; i < reversedParts.length; i++){
            if (reversedParts[i].matches("\\d+")){
                firstNumberIndex = i;
                break;
            }
        }

        //length of the array
        int lengthOfArray = reversedParts.length;
        int indexInNotReversedArray = lengthOfArray - firstNumberIndex - 1;

        //System.out.println("firstNonNumberIndex is: " + indexInNotReversedArray);

        //split the array into two parts from that index

        String[] arr1 = Arrays.copyOfRange(parts, 0, indexInNotReversedArray);
        String[] arr2 = Arrays.copyOfRange(parts, indexInNotReversedArray, parts.length);

   //     System.out.println("arr1 is: " + Arrays.toString(arr1));
    //    System.out.println("arr2 is: " + Arrays.toString(arr2));


    int numOfFilesToDelete = Integer.parseInt(arr2[0]);
    //add the files to delete from arr2 to the filesToDelete list
        for (int i = 0; i < numOfFilesToDelete; i++){
            filesToDelete.add(arr2[i + 1]);
        }
        System.out.println("filesToDelete is: " + filesToDelete);

        //delete the first element of arr1
        String[] arr1WithoutFirstTwoElements = Arrays.copyOfRange(arr1, 2, arr1.length);
        System.out.println("arr1WithoutFirstTwoElements is: " + Arrays.toString(arr1WithoutFirstTwoElements));

//find the number of files to send
        int numOfFilesToSend = Integer.parseInt(arr1[1]);
        System.out.println("numOfFilesToSend is: " + numOfFilesToSend);

        //iterate over the arr1WithoutFirstElement array and extract the files to send and add them to the filesToSend map

        for (int i =0 ; i < numOfFilesToSend; i++){
            //get the file name
            String fileName = arr1WithoutFirstTwoElements[0];
            //add the file name to the filesToSend map
            filesToSend.put(fileName, new ArrayList<>());

            int numOfPeers = Integer.parseInt(arr1WithoutFirstTwoElements[1]);

            for (int j = 2; j < numOfPeers + 2; j++){
                filesToSend.get(fileName).add(Integer.parseInt(arr1WithoutFirstTwoElements[j]));
            }

            //remove form the arr1WithoutFirstTwoElements array the file name and the number of peers
            arr1WithoutFirstTwoElements = Arrays.copyOfRange(arr1WithoutFirstTwoElements, numOfPeers + 2, arr1WithoutFirstTwoElements.length);
        }


        System.out.println("filesToSend is: " + filesToSend);












    }







}
