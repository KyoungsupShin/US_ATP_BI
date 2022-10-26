cd C:/Users/qcells/Desktop/US_ATP_BI

set mytime=%date%

git init
git rm -r --cached .
git pull https://github.com/KyoungsupShin/US_ATP_BI.git dev
git add . 
git commit -m "%mytime%" 
git push

timeout /t 10 /nobreak