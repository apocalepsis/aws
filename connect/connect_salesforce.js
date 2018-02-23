var util = require('util');
var request = require('request');

var sf_client_id = '*****'
var sf_client_secret = '*****';
var sf_username = '*****';
var sf_password = '*****!';

function parse_date_to_spanish_str(date) {

    var months = ['','Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
    var response = 'Día %s de %s de %s';
    var date_array = date.split('-');
    response = util.format(response,parseInt(date_array[2]),months[parseInt(date_array[1])],parseInt(date_array[0]));

    return response;

}

exports.handler = (event, context, callback) => {

    var request_options = {
        method: 'POST',
        url: 'https://na73.salesforce.com/services/oauth2/token',
        headers:{
            "content-type": 'application/x-www-form-urlencoded'
        },
        form: {
            'grant_type' : 'password',
            'client_id' : sf_client_id,
            'client_secret' : sf_client_secret,
            'username' : sf_username,
            'password' : sf_password
        }
    };

    request(request_options, function(error,response,body){

        if (response && response.statusCode == 200) {

            var json_body = JSON.parse(body);

            if (json_body.access_token) {

                console.log(json_body.access_token);

                var credit_number = event['Details']['Parameters']['credit_number'];
                var get_credit_info = event['Details']['Parameters']['get_credit_info'];

                console.log('CREDIT NUMBER: ',credit_number);
                console.log('CREDIT INFO: ',get_credit_info);

                var sf_query_object = 'Credit__c';
                var sf_query_field = 'Name';
                var sf_query_filter = 'Name';

                if (get_credit_info == 'amount')
                    sf_query_field = 'Credit_Amount__c';
                else if (get_credit_info == 'status')
                    sf_query_field = 'Credit_Status__c';
                else if (get_credit_info == 'next_payment_date')
                    sf_query_field = 'Credit_Next_Payment_Date__c';
                else {
                    console.log('error: invalid input');
                    callback({},null);
                }

                var sf_query_url = util.format(
                    'https://na73.salesforce.com/services/data/v20.0/query/?q=SELECT %s FROM %s WHERE Name=\'%s\'',
                    sf_query_field,sf_query_object,credit_number.replace('.',''));

                request_options = {
                    method : 'GET',
                    url : sf_query_url,
                    headers:{
                        "Authorization" : 'Bearer ' + json_body.access_token
                    }
                };

                request(request_options, function(e,r,b){
                    if (e) {
                        console.log('error:',e);
                        callback({},null);
                    } else {
                        console.log('response:', response && response.statusCode);
                        console.log('body:',b);
                        var json_b = JSON.parse(b);
                        var callback_response = {};
                        if (parseInt(json_b.totalSize) == 1) {
                            if (get_credit_info == 'next_payment_date') {
                                callback_response = {
                                    'status' : '1',
                                    'payload' : parse_date_to_spanish_str(json_b.records[0][sf_query_field])
                                };
                            } else {
                                callback_response = {
                                    'status' : '1',
                                    'payload' : json_b.records[0][sf_query_field]
                                };
                            }
                            callback(null,callback_response)
                        } else {
                            callback_response = {
                                'status' : '0',
                                'payload' : 'Credit not found'
                            };
                        }
                        callback(null,callback_response);
                    }
                });

            } else {
                console.log('error: access token not found');
                callback({},null);
            }

        } else {
            console.log('error:', error);
            callback({},null);
        }

    });

};
