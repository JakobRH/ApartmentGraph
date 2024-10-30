const willhaben = require('../willhaben/node_modules/willhaben')
const fs = require('fs');

for (let i = 1; i < 2; i++) {
    willhaben.getListings('https://www.willhaben.at/iad/immobilien/eigentumswohnung/wien?rows=90&page=' + i).then(json => {
        fs.writeFile('./'+i+'.json', JSON.stringify(json), err => {
            if (err) {
                console.error(err);
            }
            // file written successfully
        });
    })
} 
