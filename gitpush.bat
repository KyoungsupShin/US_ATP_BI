cd C:/Users/qcells/Desktop/ATP/US_ATP_BI

set mytime=%date%

git init
git pull https://github.com/KyoungsupShin/US_ATP_BI.git dev
git add . 
git commit -m "%mytime%" 
git push

timeout /t 10 /nobreak