rm -fv read mate full_match errors.dump
for t in t001 t002; do
    rm -fv ${t}_read ${t}_mate ${t}_errors
done
