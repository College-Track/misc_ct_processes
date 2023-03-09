function howToCopyMessage() {
  var result = SpreadsheetApp.getUi().alert("To paste data enter: \n \n on Mac: CMD + Shift + V \n on Windows: Ctrl + Shift + V");
  if(result === SpreadsheetApp.getUi().Button.OK) {
    //Take some action
    SpreadsheetApp.getActive().toast("Notice a flag on the top right corner of your cell? \n Click the dropdown arrow in the cell to select from the menu. Or it won't make it to Salesforce :(", "Hey!",-1); //little note on bottom right
    //SpreadsheetApp.getActive().toast("Message", "Title", 15);
    Utilities.sleep(300000); // 5 minutes 

  SpreadsheetApp.getActive().toast("To Paste on Mac: CMD + Shift + V on Windows: Ctrl + Shift + V","Remember!", -1);//message 2 interrupts message 1
  }

}
