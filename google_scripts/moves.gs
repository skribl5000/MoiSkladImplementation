function onOpen(e)
{
  SpreadsheetApp.getUi()
    .createMenu('Меню')
    .addItem('Перемещения', 'movements')
    .addToUi();
}

function movements() {
  let token = '';
  let moves_url = 'https://online.moysklad.ru/api/remap/1.2/entity/move';
  let headers_auth = {Authorization: `Basic ${token}`};
  let moves_response = UrlFetchApp.fetch(moves_url, {'headers':headers_auth});

  var stores_manager = new StoresManager(token);

  let json = moves_response.getContentText();
  var data = JSON.parse(json);
  var moves = data['rows']

  let table_headers = ['boxNumber', 'sourceStore', 'targetStore', 'actionDate', 'barcode', 'quantity'];
  let table = [
    table_headers,
  ]

  for (const [_, moveItem] of Object.entries(moves)){

    let move = new MSMove(moveItem, token, stores_manager);
    let boxNumber = move.boxNumber;
    let sourceStore = move.sourceStoreName;
    let targetStore = move.targetStoreName;
    let actionDate = move.actionDate;

    for (const [_, position] of Object.entries(move.positions)){

      let barcode = position.barcode;
      let quantity = position.quantity;

      table.push([
        boxNumber, sourceStore, targetStore,
        actionDate, barcode, quantity
      ])
    }
  }

  let sheet = SpreadsheetApp.getActiveSheet();
  var ui = SpreadsheetApp.getUi();

  if (sheet.getName() != 'Перемещения'){
    ui.alert('Неверный лист. Необходимо находится на листе "Перемещения".')
  }
  else{
    sheet.clearContents();
    let height = table.length;
    let width = table[0].length;
    var range = sheet.getRange(2,1,height,width).setValues(table);
  }
}

class StoresManager {

   constructor(token){
     this.ALL_STORES_REQUEST_URL = "https://online.moysklad.ru/api/remap/1.2/entity/store";
     this.token=token;
     this.data = this.getStoresData();
     this.hrefNameMap = this.getStoresNameMap();
   };

  getStoresData(){
    const request_url = this.ALL_STORES_REQUEST_URL;
    const headers = {'Authorization':`Basic ${this.token}`};
    const options = {'headers':headers}
    let response = UrlFetchApp.fetch(request_url, options);
    let json = response.getContentText();
    return JSON.parse(json);
  }

  getStoresNameMap(){
    let result = {};
    const stores = this.data['rows'];

    if (typeof stores == 'undefined'){
      return result
    }
    for (const [_, store] of Object.entries(stores)){
        result[store['meta']['href']] = store['name'];
    }
    return result;
  };

  getStoreNameByHref(storeHref){
    return this.hrefNameMap[storeHref];
  };
};

class MSMovePosition{
  constructor(data, token){
    this.authHeader = {'Authorization': `Basic ${token}`};
    this.quantity = this.getQuantityFromData(data);
    this.positionInfo = this.getPositionInfo(data);
  }

  get barcode(){
    const barcodes = this.positionInfo['barcodes'];
    for (const [_, barcode] of Object.entries(barcodes)){
      let code = barcode['ean13'];
      if (code != 'undefined'){
        return code;
      }
    };
  }

  get productName(){
    return this.positionInfo['name'];
  };

  get externalCode(){
    return this.positionInfo['externalCode'];
  };

  getQuantityFromData(data){
    let quantity = data['quantity'];
    if (typeof quantity != 'undefined'){
      return quantity;
    }
  }

  getPositionInfo(data){
    const posMeta = data['meta'];
    let positionDataRequestUrl = posMeta['href'];
    if (typeof positionDataRequestUrl == 'undefined'){
      return;
    }

    const positionInfo = UrlFetchApp.fetch(positionDataRequestUrl, {'headers':this.authHeader});
    const positionInfoJson = JSON.parse(positionInfo.getContentText());

    const productsAssortiment = positionInfoJson['assortment'];
    if (typeof productsAssortiment == 'undefined'){
      return {};
    }

    const productDataResponse = UrlFetchApp.fetch(productsAssortiment['meta']['href'], {'headers': this.authHeader});
    const resultJson = JSON.parse(productDataResponse);
    return resultJson;
  };
}

class MSMove {

  constructor(data, token, stores_manager) {
  this.token = token;
  this.storesManager = stores_manager;
  this.MAIN_STORE_URL = "https://online.moysklad.ru/api/remap/1.2/entity/store/82b96dcf-58a3-11eb-0a80-022e004085ff";
  this.BOX_STORE_URL = "https://online.moysklad.ru/api/remap/1.2/entity/store/be5071a7-61ae-11eb-0a80-06ae0001c060";

  this.boxNumber = this.getBoxNumberFromData(data);
  this.positions = this.getPositionListByData(data);
  this.sourceStoreHref = data['sourceStore']['meta']['href'];
  this.targetStoreHref = data['targetStore']['meta']['href'];
  this.actionDate = this.getCreatedDateFromData(data);

  this.authHeader = {'Authorization': `Basic ${token}`};
  };

  get boxnum(){
    return this.boxNumber;
  };

  get targetStoreName(){
    return this.storesManager.getStoreNameByHref(this.targetStoreHref);
  };
  get sourceStoreName(){
    return this.storesManager.getStoreNameByHref(this.sourceStoreHref);
  }

  getBoxNumberFromData(data){
    const attributes = data['attributes'];
    if (typeof attributes == 'undefined'){
      return '';
    };

    for (const [_, element] of Object.entries(attributes)) {
      if (element['name'] == 'Номер короба'){
         return element['value'];
      };
    };
  };

  getCreatedDateFromData(data){
    let date = data['created'];
    if (typeof date == 'undefined'){
      return ''
    }
    if (typeof date == 'string'){
      let splited_date = date.split(' ')
      return splited_date[0];
    }
  };

  getPositionListByData(data){
    let result = [];

    const positionsInfo = data['positions'];
    const positionsMeta = positionsInfo['meta'];
    const positionsRequestUrl = positionsMeta['href'];
    let positionsData = UrlFetchApp.fetch(positionsRequestUrl, {'headers':{'Authorization': `Basic ${this.token}`}});
    let positionsJson = JSON.parse(positionsData.getContentText());

    if (typeof positionsJson['meta'] == 'undefined'){
      return result;
    }
    if (positionsJson['meta']['size'] == 0){
      return result;
    }
    const rows = positionsJson['rows'];
    for (const [_, positionElement] of Object.entries(rows)){
      let pos = new MSMovePosition(positionElement, this.token);

      result.push(pos);
    }
    return result;
  }

}
