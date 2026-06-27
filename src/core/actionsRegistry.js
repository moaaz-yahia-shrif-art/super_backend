const db = require("../database/mockDB");

module.exports = {
  getData: (data) => require("../actions/getData")(data, db),
  postData: (data) => require("../actions/postData")(data, db),
  deleteData: (data) => require("../actions/deleteData")(data, db),
};
