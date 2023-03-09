function onEdit(e) {

// sheets variables  
const range = e.range;
var activeSheet = e.source.getActiveSheet();  // identify sheet you are on
var newValue = e.range.getValues();
var oldValue = e.oldValue;
const row = e.range.getRow();
const col = e.range.getColumn();
var activeRange = e.source.getActiveRange();
var index = activeRange.getRowIndex();
var sheet = e.source.getActiveSheet(); // dupe?
var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues();

var userCol = headers[0].indexOf('User');        // identify User column; var col = data[0].indexOf(COLUMN_NAME_HERE);
var ctEditCol = headers[0].indexOf('EditTracker'); // identify Timestamp column

var fillctEdit = sheet.getRange(index, ctEditCol + 1);

// generate user email upon edit
 const USER_EMAIL_COLUMN = e.source.getActiveSheet().getLastColumn();
 const email= Session.getActiveUser().getEmail();

  if (activeSheet.getSheetName() == "READ" || 
      (email == "ireyna@collegetrack.org" ||
       email == "llaracortes@collegetrack.org" ||
       email == "breneckar@collegetrack.org")
      ){  // if the tab being edited is the READ tab, exit trigger
    return;
  }  
// if the edited value is different from the initial value, track changes
if (oldValue != newValue && !range.isBlank()){     // is there a change in value in the cell? Or is whatever was entered the same as before
    range.setBackground(null);                     // no background color
    range.setFontColor('green');                   // change text to green when there is a change
    fillctEdit.setValue('1');
    var cellUser = sheet.getRange(index, userCol + 1);
    if (userCol > -1 && index > 1) { // only return email if 'User' header exists, but not in the header row itself!
      e.source.getActiveSheet().getRange(row,userCol + 1).setValue(email);
      cellUser.setValue(email);
      }
   }

if (oldValue != newValue &&     // if oldValue was NOT null, and is changed to NULL, cell color changes to gray 
    oldValue != null && 
    range.isBlank()){   
    range.setBackground('gray');
    fillctEdit.setValue('1');
    fillEmail.setValue(email);
    if (userCol > -1 && index > 1) { // only return email if 'User' header exists, but not in the header row itself!
      var cellUser = sheet.getRange(index, userCol + 1);
      e.source.getActiveSheet().getRange(row,userCol + 1).setValue(email);
      cellUser.setValue(email);
      }
    }
}
  
