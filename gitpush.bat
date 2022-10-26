cd C:/Users/qcells/Desktop/US_ATP_BI

set mytime=%date%

git init
git pull https://github.com/KyoungsupShin/US_ATP_BI.git dev2
git add . 
git commit -m "%mytime%" 
git push

timeout /t 10 /nobreak