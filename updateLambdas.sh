
cd deployment
cp ./../StateLambda/lambda_function.py ./
cp ./../Common/*.py ./

zip -r9 ./package.zip .
aws lambda update-function-code --function-name StateLambda --zip-file fileb://package.zip --region eu-central-1

cd ..

cd deployment
cp ./../MapLambda/lambda_function.py ./
cp ./../Common/*.py ./

zip -r9 ./package.zip .
aws lambda update-function-code --function-name MapLambda --zip-file fileb://package.zip --region eu-central-1

cd ..

cd deployment
cp ./../ReduceLambda/lambda_function.py ./
cp ./../Common/*.py ./

zip -r9 ./package.zip .
aws lambda update-function-code --function-name ReduceLambda --zip-file fileb://package.zip --region eu-central-1

cd ..